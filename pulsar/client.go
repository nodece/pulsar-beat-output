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
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/elastic/beats/v9/libbeat/beat"
	"github.com/elastic/beats/v9/libbeat/outputs"
	"github.com/elastic/beats/v9/libbeat/outputs/codec"
	"github.com/elastic/beats/v9/libbeat/outputs/outil"
	"github.com/elastic/beats/v9/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/logp"
)

type client struct {
	clientOptions        pulsar.ClientOptions
	producerOptions      pulsar.ProducerOptions
	pulsarClient         pulsar.Client
	observer             outputs.Observer
	beat                 beat.Info
	config               *pulsarConfig
	codec                codec.Codec
	topicSelector        outil.Selector
	partitionKeySelector outil.Selector
	producers            *Producers
	logger               *logp.Logger
}

var _ outputs.NetworkClient = &client{}

func newPulsarClient(
	beat beat.Info,
	observer outputs.Observer,
	clientOptions pulsar.ClientOptions,
	producerOptions pulsar.ProducerOptions,
	config *pulsarConfig,
	topicSelector outil.Selector,
	partitionKeySelector outil.Selector,
) (*client, error) {
	logger := beat.Logger.Named("Pulsar")
	c := &client{
		clientOptions:        clientOptions,
		producerOptions:      producerOptions,
		observer:             observer,
		beat:                 beat,
		config:               config,
		topicSelector:        topicSelector,
		partitionKeySelector: partitionKeySelector,
		producers:            NewProducers(logger, config.MaxCacheProducers),
		logger:               logger,
	}
	return c, nil
}

func (c *client) Connect(_ context.Context) error {
	var err error
	c.pulsarClient, err = pulsar.NewClient(c.clientOptions)
	c.logger.Info("start create pulsar client")
	if err != nil {
		c.logger.Debugf("Create pulsar client failed: %v", err)
		return err
	}

	c.logger.Info("start create encoder")
	c.codec, err = codec.CreateEncoder(c.beat, c.config.Codec)
	if err != nil {
		c.logger.Debugf("Create encoder failed: %v", err)
		return err
	}

	return nil
}

func (c *client) Close() error {
	c.producers.Close()
	if c.pulsarClient != nil {
		c.pulsarClient.Close()
	}
	c.logger.Info("pulsar client closed")
	return nil
}

func (c *client) Publish(_ context.Context, batch publisher.Batch) error {
	defer batch.ACK()
	events := batch.Events()
	c.observer.NewBatch(len(events))
	c.logger.Debugf("Received events: %d", len(events))
	for i := range events {
		event := &events[i]
		serializedEvent, err := c.codec.Encode(c.beat.Beat, &event.Content)
		if err != nil {
			c.observer.PermanentErrors(1)
			c.logger.Error(err)
			continue
		}
		buf := make([]byte, len(serializedEvent))
		copy(buf, serializedEvent)
		c.logger.Debugf("Success encode events: %d", i)
		topic := selectTopic(&event.Content, c)
		producer, err := c.producers.LoadProducer(topic, c)
		if err != nil {
			c.logger.Errorf("load pulsar producer{topic=%s} failed: %v", topic, err)
			return err
		}

		pTime := time.Now()
		partitionKey := selectPartitionKey(&event.Content, pTime, c)
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			EventTime: pTime,
			Key:       partitionKey,
			Payload:   buf,
		}, func(msgId pulsar.MessageID, prodMsg *pulsar.ProducerMessage, err error) {
			if err != nil {
				c.observer.PermanentErrors(1)
				c.logger.Error(err)
			} else {
				c.logger.Debugf("Pulsar success send events: messageID: %s ", msgId)
				c.observer.AckedEvents(1)
			}
		})
	}
	c.logger.Debugf("Success send events: %d", len(events))
	return nil
}

func (c *client) String() string {
	return "file(" + c.clientOptions.URL + ")"
}

func selectTopic(event *beat.Event, client *client) string {
	topic, err := client.topicSelector.Select(event)
	if err != nil {
		client.logger.Errorf("select topic failed with %v", err)
	}
	if topic == "" {
		topic = client.producerOptions.Topic
	}
	client.logger.Debugf("Selected topic: %s", topic)

	return topic
}

func selectPartitionKey(event *beat.Event, eventTime time.Time, client *client) string {
	partitionKey, err := client.partitionKeySelector.Select(event)
	if err != nil {
		client.logger.Errorf("select partitionKey failed with %v", err)
	}
	if partitionKey == "" {
		partitionKey = fmt.Sprintf("%d", eventTime.Nanosecond())
	}
	client.logger.Debugf("Selected partitionKey: %s", partitionKey)

	return partitionKey
}
