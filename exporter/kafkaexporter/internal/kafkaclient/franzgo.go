// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaclient // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/kafkaclient"

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// FranzSyncProducer is a wrapper around the franz-go client that implements
// the Producer interface. Allowing us to use the franz-go client while
// maintaining compatibility with the existing Kafka exporter code.
type FranzSyncProducer struct {
	client       *kgo.Client
	metadataKeys []string
}

// NewFranzSyncProducer Franz-go producer from a kgo.Client and a Messenger.
func NewFranzSyncProducer(client *kgo.Client,
	metadataKeys []string,
) *FranzSyncProducer {
	return &FranzSyncProducer{
		client:       client,
		metadataKeys: metadataKeys,
	}
}

// ExportData sends a batch of messages to Kafka
func (p *FranzSyncProducer) ExportData(ctx context.Context, msgs Messages) error {
	messages := makeFranzMessages(msgs)
	setMessageHeaders(ctx, messages, p.metadataKeys,
		func(key string, value []byte) kgo.RecordHeader {
			return kgo.RecordHeader{Key: key, Value: value}
		},
		func(m *kgo.Record) []kgo.RecordHeader { return m.Headers },
		func(m *kgo.Record, h []kgo.RecordHeader) { m.Headers = h },
	)
	result := p.client.ProduceSync(ctx, messages...)
	var errs []error
	for _, r := range result {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errors.Join(errs...)
}

// Close shuts down the producer and flushes any remaining messages.
func (p *FranzSyncProducer) Close() error {
	p.client.Close()
	return nil
}

// FranzAsyncProducer is a wrapper around the franz-go client that implements
// the Producer interface for asynchronous producing.
type FranzAsyncProducer struct {
	client       *kgo.Client
	logger       *zap.Logger
	metadataKeys []string
}

// NewFranzAsyncProducer creates a new FranzAsyncProducer.
func NewFranzAsyncProducer(
	client *kgo.Client,
	logger *zap.Logger,
	metadataKeys []string,
) *FranzAsyncProducer {
	return &FranzAsyncProducer{
		client:       client,
		logger:       logger,
		metadataKeys: metadataKeys,
	}
}

// ExportData sends a batch of messages to Kafka asynchronously.
func (p *FranzAsyncProducer) ExportData(ctx context.Context, msgs Messages) error {
	messages := makeFranzMessages(msgs)
	setMessageHeaders(ctx, messages, p.metadataKeys,
		func(key string, value []byte) kgo.RecordHeader {
			return kgo.RecordHeader{Key: key, Value: value}
		},
		func(m *kgo.Record) []kgo.RecordHeader { return m.Headers },
		func(m *kgo.Record, h []kgo.RecordHeader) { m.Headers = h },
	)

	for _, msg := range messages {
		p.client.Produce(ctx, msg, func(r *kgo.Record, err error) {
			if err != nil {
				p.logger.Error("Failed to produce message to Kafka",
					zap.Error(err),
					zap.String("topic", r.Topic),
					zap.Int32("partition", r.Partition),
				)
			}
		})
	}
	return nil
}

// Close shuts down the producer and flushes any remaining messages.
func (p *FranzAsyncProducer) Close() error {
	p.client.Close()
	return nil
}

func makeFranzMessages(messages Messages) []*kgo.Record {
	msgs := make([]*kgo.Record, 0, messages.Count)
	for _, msg := range messages.TopicMessages {
		for _, message := range msg.Messages {
			msg := &kgo.Record{Topic: msg.Topic}
			if message.Key != nil {
				msg.Key = message.Key
			}
			if message.Value != nil {
				msg.Value = message.Value
			}
			msgs = append(msgs, msg)
		}
	}
	return msgs
}
