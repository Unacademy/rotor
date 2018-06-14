/*
Copyright 2018 Turbine Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package adapter

import (
	"time"

	envoyapi "github.com/envoyproxy/go-control-plane/envoy/api/v2"
	envoyauth "github.com/envoyproxy/go-control-plane/envoy/api/v2/auth"
	envoycluster "github.com/envoyproxy/go-control-plane/envoy/api/v2/cluster"
	envoycore "github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	"github.com/envoyproxy/go-control-plane/pkg/cache"

	tbnapi "github.com/turbinelabs/api"
	"github.com/turbinelabs/rotor/xds/poller"
	"github.com/go-redis/redis"
	"encoding/json"
	google_protobuf2 "github.com/gogo/protobuf/types"
)

const (
	clusterConnectTimeoutSecs = 10
	configRedisKeyPrefix = "rotor_cds_"
	http1 = "http1"
	http2 = "http2"
	gRPC = "grpc"
)

type clusterConfigParams struct {
	Protocol					string	`json:"protocol"`  //Can be http1, http2, grpc. Required.
	EnableHealthCheck			bool	`json:"enable_health_check"`		//Defaults to false
	HttpHealthCheckUrl			string	`json:"http_health_check_url"`
	GrpcHealthCheckServiceName	string	`json:"grpc_health_check_service_name"`	//Defaults to ""
}

type cds struct {
	caFile string
	env string
	redis *redis.Client
}

func newCds(caFile string, redisAddr string, env string) cds {
	return cds{
		caFile: caFile,
		env: env,
		redis: redis.NewClient(&redis.Options{Addr:redisAddr, Password:"", DB:0, PoolSize:10}),
	}
}

// resourceAdapter turns poller.Objects into Cluster cache.Resources
func (s cds) resourceAdapter(objects *poller.Objects) (cache.Resources, error) {
	resources := make(map[string]cache.Resource, len(objects.Clusters))
	for _, cluster := range objects.Clusters {
		envoyCluster, err := s.tbnToEnvoyCluster(cluster, objects)
		if err != nil {
			return cache.Resources{}, err
		}
		resources[envoyCluster.GetName()] = envoyCluster
	}
	return cache.Resources{Version: objects.TerribleHash(), Items: resources}, nil
}

func (s cds) tbnToEnvoyCluster(
	tbnCluster tbnapi.Cluster,
	objects *poller.Objects,
) (*envoyapi.Cluster, error) {
	subsets := objects.SubsetsPerCluster(tbnCluster.ClusterKey)

	var subsetConfig *envoyapi.Cluster_LbSubsetConfig

	if subsets != nil {
		subsetSelectors :=
			make([]*envoyapi.Cluster_LbSubsetConfig_LbSubsetSelector, 0, len(subsets))

		for _, subset := range subsets {
			subsetSelectors = append(
				subsetSelectors,
				&envoyapi.Cluster_LbSubsetConfig_LbSubsetSelector{Keys: subset},
			)
		}
		subsetConfig = &envoyapi.Cluster_LbSubsetConfig{
			FallbackPolicy:  envoyapi.Cluster_LbSubsetConfig_ANY_ENDPOINT,
			SubsetSelectors: subsetSelectors,
		}
	}

	var tlsContext *envoyauth.UpstreamTlsContext
	if tbnCluster.RequireTLS {
		tlsContext = &envoyauth.UpstreamTlsContext{
			CommonTlsContext: &envoyauth.CommonTlsContext{
				TlsParams: &envoyauth.TlsParameters{
					TlsMinimumProtocolVersion: envoyauth.TlsParameters_TLS_AUTO,
					TlsMaximumProtocolVersion: envoyauth.TlsParameters_TLS_AUTO,
				},
			},
			Sni: tbnCluster.Name,
		}

		if s.caFile != "" {
			tlsContext.CommonTlsContext.ValidationContext = &envoyauth.CertificateValidationContext{
				TrustedCa: &envoycore.DataSource{
					Specifier: &envoycore.DataSource_Filename{
						Filename: s.caFile,
					},
				},
			}
		}
	}

	configParams, err := configForCluster(s.redis, s.env, tbnCluster.Name)
	if err != nil {
		return nil, err
	}

	cluster := &envoyapi.Cluster{
		Name: tbnCluster.Name,
		Type: envoyapi.Cluster_EDS,
		EdsClusterConfig: &envoyapi.Cluster_EdsClusterConfig{
			EdsConfig:   &xdsClusterConfig,
			ServiceName: tbnCluster.Name,
		},
		ConnectTimeout:   clusterConnectTimeoutSecs * time.Second,
		LbPolicy:         envoyapi.Cluster_LEAST_REQUEST,
		TlsContext:       tlsContext,
		LbSubsetConfig:   subsetConfig,
		CircuitBreakers:  tbnToEnvoyCircuitBreakers(tbnCluster.CircuitBreakers),
		OutlierDetection: tbnToEnvoyOutlierDetection(tbnCluster.OutlierDetection),
	}

	if configParams.Protocol == http2 || configParams.Protocol == gRPC {
		cluster.Http2ProtocolOptions = &envoycore.Http2ProtocolOptions{}
	}

	if configParams.EnableHealthCheck {
		hc := &envoycore.HealthCheck{
			Timeout: &google_protobuf2.Duration{Nanos:100000000},
			Interval: &google_protobuf2.Duration{Seconds:30},
		}
		if configParams.Protocol == http1 || configParams.Protocol == http2 {
			hc.HealthChecker = &envoycore.HealthCheck_HttpHealthCheck_{HttpHealthCheck:&envoycore.HealthCheck_HttpHealthCheck{Path: configParams.HttpHealthCheckUrl}}
		} else {
			hc.HealthChecker = &envoycore.HealthCheck_GrpcHealthCheck_{GrpcHealthCheck:&envoycore.HealthCheck_GrpcHealthCheck{ServiceName: configParams.GrpcHealthCheckServiceName}}
		}
		cluster.HealthChecks = []*envoycore.HealthCheck{hc}
	}

	return cluster, nil
}

func tbnToEnvoyCircuitBreakers(tbnCb *tbnapi.CircuitBreakers) *envoycluster.CircuitBreakers {
	if tbnCb == nil {
		return nil
	}

	return &envoycluster.CircuitBreakers{
		Thresholds: []*envoycluster.CircuitBreakers_Thresholds{
			{
				Priority:           defaultRoutingPriority,
				MaxConnections:     intPtrToUint32Ptr(tbnCb.MaxConnections),
				MaxPendingRequests: intPtrToUint32Ptr(tbnCb.MaxPendingRequests),
				MaxRetries:         intPtrToUint32Ptr(tbnCb.MaxRetries),
				MaxRequests:        intPtrToUint32Ptr(tbnCb.MaxRequests),
			},
		},
	}
}

func envoyToTbnCircuitBreakers(ecb *envoycluster.CircuitBreakers) *tbnapi.CircuitBreakers {
	if ecb == nil {
		return nil
	}

	for _, cbt := range ecb.GetThresholds() {
		if cbt.GetPriority() == envoycore.RoutingPriority_DEFAULT {
			return &tbnapi.CircuitBreakers{
				MaxConnections:     uint32PtrToIntPtr(cbt.GetMaxConnections()),
				MaxPendingRequests: uint32PtrToIntPtr(cbt.GetMaxPendingRequests()),
				MaxRequests:        uint32PtrToIntPtr(cbt.GetMaxRequests()),
				MaxRetries:         uint32PtrToIntPtr(cbt.GetMaxRetries()),
			}
		}
	}

	return nil
}

func tbnToEnvoyOutlierDetection(tod *tbnapi.OutlierDetection) *envoycluster.OutlierDetection {
	if tod == nil {
		return nil
	}

	return &envoycluster.OutlierDetection{
		Consecutive_5Xx:                    intPtrToUint32Ptr(tod.Consecutive5xx),
		Interval:                           intPtrToDurationPtr(tod.IntervalMsec),
		BaseEjectionTime:                   intPtrToDurationPtr(tod.BaseEjectionTimeMsec),
		MaxEjectionPercent:                 intPtrToUint32Ptr(tod.MaxEjectionPercent),
		EnforcingConsecutive_5Xx:           intPtrToUint32Ptr(tod.EnforcingConsecutive5xx),
		EnforcingSuccessRate:               intPtrToUint32Ptr(tod.EnforcingSuccessRate),
		SuccessRateMinimumHosts:            intPtrToUint32Ptr(tod.SuccessRateMinimumHosts),
		SuccessRateRequestVolume:           intPtrToUint32Ptr(tod.SuccessRateRequestVolume),
		SuccessRateStdevFactor:             intPtrToUint32Ptr(tod.SuccessRateStdevFactor),
		ConsecutiveGatewayFailure:          intPtrToUint32Ptr(tod.ConsecutiveGatewayFailure),
		EnforcingConsecutiveGatewayFailure: intPtrToUint32Ptr(tod.EnforcingConsecutiveGatewayFailure),
	}
}

func envoyToTbnOutlierDetection(eod *envoycluster.OutlierDetection) *tbnapi.OutlierDetection {
	if eod == nil {
		return nil
	}

	return &tbnapi.OutlierDetection{
		IntervalMsec:                       durationPtrToIntPtr(eod.GetInterval()),
		BaseEjectionTimeMsec:               durationPtrToIntPtr(eod.GetBaseEjectionTime()),
		MaxEjectionPercent:                 uint32PtrToIntPtr(eod.GetMaxEjectionPercent()),
		Consecutive5xx:                     uint32PtrToIntPtr(eod.GetConsecutive_5Xx()),
		EnforcingConsecutive5xx:            uint32PtrToIntPtr(eod.GetEnforcingConsecutive_5Xx()),
		EnforcingSuccessRate:               uint32PtrToIntPtr(eod.GetEnforcingSuccessRate()),
		SuccessRateMinimumHosts:            uint32PtrToIntPtr(eod.GetSuccessRateMinimumHosts()),
		SuccessRateRequestVolume:           uint32PtrToIntPtr(eod.GetSuccessRateRequestVolume()),
		SuccessRateStdevFactor:             uint32PtrToIntPtr(eod.GetSuccessRateStdevFactor()),
		ConsecutiveGatewayFailure:          uint32PtrToIntPtr(eod.GetConsecutiveGatewayFailure()),
		EnforcingConsecutiveGatewayFailure: uint32PtrToIntPtr(eod.GetEnforcingConsecutiveGatewayFailure()),
	}
}

func configForCluster(client *redis.Client, env string, clusterName string) (*clusterConfigParams, error) {
	key := configRedisKeyPrefix + env + "_" + clusterName
	config, err := client.Get(key).Result()

	if err != nil {
		return nil, err
	}

	var configParams clusterConfigParams
	err = json.Unmarshal([]byte(config), &configParams)

	return &configParams, err
}