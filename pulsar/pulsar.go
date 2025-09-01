/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package pulsar

import (
	"github.com/elastic/beats/v9/libbeat/beat"
	"github.com/elastic/beats/v9/libbeat/outputs"
	"github.com/elastic/beats/v9/libbeat/outputs/outil"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
)

func init() {
	outputs.RegisterType("pulsar", makePulsar)
}

func makePulsar(__ outputs.IndexManager, beat beat.Info, observer outputs.Observer, cfg *config.C) (outputs.Group, error) {
	config := defaultConfig()
	beat.Logger.Info("initialize pulsar output")
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	beat.Logger.Infof("init config %v", config)
	clientOptions, producerOptions, err := initOptions(&config)
	if err != nil {
		return outputs.Fail(err)
	}

	topicSelector, err := buildTopicSelector(cfg, beat.Logger)
	if err != nil {
		return outputs.Fail(err)
	}

	partitionKeySelector, err := buildPartitionKeySelector(cfg, beat.Logger)
	if err != nil {
		return outputs.Fail(err)
	}

	client, err := newPulsarClient(beat, observer, *clientOptions, *producerOptions, &config, topicSelector, partitionKeySelector)
	if err != nil {
		return outputs.Fail(err)
	}
	retry := 0
	if config.MaxRetries < 0 {
		retry = -1
	}

	return outputs.Success(config.Queue, config.BulkMaxSize, retry, nil, beat.Logger, client)
}

func buildTopicSelector(cfg *config.C, logger *logp.Logger) (outil.Selector, error) {
	return outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "topic",
		EnableSingleOnly: true,
		FailEmpty:        true,
	}, logger)
}

func buildPartitionKeySelector(cfg *config.C, logger *logp.Logger) (outil.Selector, error) {
	return outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "partition_key",
		EnableSingleOnly: true,
		FailEmpty:        false,
	}, logger)
}
