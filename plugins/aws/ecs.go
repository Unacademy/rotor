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

package aws

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ecs"

	"github.com/turbinelabs/cli/command"
	tbnflag "github.com/turbinelabs/nonstdlib/flag"
	"github.com/turbinelabs/nonstdlib/flag/usage"
	"github.com/turbinelabs/nonstdlib/log/console"
	"github.com/turbinelabs/rotor"
	"github.com/turbinelabs/rotor/updater"
)

const ecsDefaultClusterTag = "tbn-cluster"

type ecsSettings struct {
	clusters       tbnflag.Strings
	clusterTag     string
	clusterPortTag string
}

func (cfg ecsSettings) Validate(aws awsClient) error {
	clusters, err := aws.ListClusters()
	if err != nil {
		return err
	}

	for _, c := range cfg.clusters.Strings {
		if _, match := clusters[c]; !match {
			return fmt.Errorf("ECS cluster %s was not found", c)
		}
	}

	return nil
}

type ecsRunner struct {
	cfg ecsSettings

	awsFlags     sessionFromFlags
	updaterFlags rotor.UpdaterFromFlags
}

func ECSCmd(updaterFlags rotor.UpdaterFromFlags) *command.Cmd {
	runner := &ecsRunner{}
	cmd := &command.Cmd{
		Name:        "ecs",
		Summary:     "ECS collector",
		Usage:       "[OPTIONS]",
		Description: ecsDescription,
		Runner:      runner,
	}

	runner.cfg.clusters = tbnflag.NewStrings()

	flags := tbnflag.Wrap(&cmd.Flags)

	flags.Var(
		&runner.cfg.clusters,
		"clusters",
		usage.Required(
			"a comma separated list indicating which ECS Clusters "+
				"should be examined for containers marked for inclusion as API Clusters",
		),
	)

	flags.StringVar(
		&runner.cfg.clusterTag,
		"cluster-tag",
		ecsDefaultClusterTag,
		"label indicating what API clusters an instance of this container will serve")

	runner.awsFlags = newSessionFromFlags(flags)
	runner.updaterFlags = updaterFlags

	return cmd
}

func (r ecsRunner) Run(cmd *command.Cmd, args []string) command.CmdErr {
	if err := r.updaterFlags.Validate(); err != nil {
		return cmd.BadInput(err)
	}

	awsSession := r.awsFlags.Make()
	awsSvc := newAwsClient(ecs.New(awsSession), ec2.New(awsSession))
	if err := r.cfg.Validate(awsSvc); err != nil {
		return cmd.BadInput(err)
	}

	updater, err := r.updaterFlags.Make()
	if err != nil {
		return cmd.Error(err)
	}

	ecsUpdateLoop(updater, awsSvc, r.cfg, ecsUpdateAction)

	return command.NoError()
}

func ecsUpdateLoop(updater updater.Updater, client awsClient, cfg ecsSettings, do ecsUpdaterFn) {
	for {
		do(cfg, updater, client)
		time.Sleep(updater.Delay())
	}
}

type ecsUpdaterFn func(ecsSettings, updater.Updater, awsClient)

func ecsUpdateAction(cfg ecsSettings, updater updater.Updater, aws awsClient) {
	state, err := NewECSState(aws, cfg.clusters.Strings)
	if err != nil {
		console.Error().Printf("Could not read ECS state: %v", err.Error())
		return
	}

	tagSet := state.meta.identifyTaggedItems(cfg)
	for i := len(tagSet) - 1; i >= 0; i-- {
		clusterTemplate := tagSet[i]
		if err := state.validate(clusterTemplate); err != nil {
			console.Error().Println(err)
			tagSet = append(tagSet[:i], tagSet[i+1:]...)
		}
	}

	clusters := bindClusters(cfg.clusterTag, state, tagSet)

	updater.Replace(clusters)
}
