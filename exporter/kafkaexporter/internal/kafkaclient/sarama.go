// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaclient // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/kafkaclient"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.uber.org/zap"
)

// SaramaSyncProducer is a wrapper around the kafkaclient.Producer that implements
// the sarama.SyncProducer interface. This allows us to use the new franz-go
// client while maintaining compatibility with the existing Sarama-based code.
type SaramaSyncProducer struct {
	producer     sarama.SyncProducer
	spm          SaramaProducerMetrics
	metadataKeys []string
}

// NewSaramaSyncProducer creates a new SaramaSyncProducer that wraps a kafkaclient.Producer.
func NewSaramaSyncProducer(
	producer sarama.SyncProducer,
	spm SaramaProducerMetrics,
	metadataKeys []string,
) *SaramaSyncProducer {
	return &SaramaSyncProducer{
		producer:     producer,
		spm:          spm,
		metadataKeys: metadataKeys,
	}
}

// ExportData sends multiple messages to the Kafka broker using the underlying producer.
func (p *SaramaSyncProducer) ExportData(ctx context.Context, msgs Messages) (err error) {
	messages := makeSaramaMessages(msgs)
	setMessageHeaders(ctx, messages, p.metadataKeys,
		func(key string, value []byte) sarama.RecordHeader {
			return sarama.RecordHeader{Key: []byte(key), Value: value}
		},
		func(m *sarama.ProducerMessage) []sarama.RecordHeader { return m.Headers },
		func(m *sarama.ProducerMessage, h []sarama.RecordHeader) { m.Headers = h },
	)
	defer p.spm.ReportProducerMetrics(ctx, messages, err, time.Now())
	if err = p.producer.SendMessages(messages); err != nil {
		err = wrapKafkaProducerError(err)
	}
	return err
}

// Close shuts down the producer and flushes any remaining messages.
// It implements the sarama.SyncProducer interface.
func (p *SaramaSyncProducer) Close() error {
	return p.producer.Close()
}

// SaramaAsyncProducer is a wrapper around sarama.AsyncProducer.
type SaramaAsyncProducer struct {
	producer     sarama.AsyncProducer
	logger       *zap.Logger
	metadataKeys []string
	wg           sync.WaitGroup
	spm          SaramaProducerMetrics
}

// NewSaramaAsyncProducer creates a new SaramaAsyncProducer that wraps a sarama.AsyncProducer.
func NewSaramaAsyncProducer(
	producer sarama.AsyncProducer,
	logger *zap.Logger,
	metadataKeys []string,
	spm SaramaProducerMetrics,
) *SaramaAsyncProducer {
	p := &SaramaAsyncProducer{
		producer:     producer,
		logger:       logger,
		metadataKeys: metadataKeys,
		spm:          spm,
	}
	p.wg.Add(2)
	go p.handleSuccesses()
	go p.handleErrors()
	return p
}

func (p *SaramaAsyncProducer) handleSuccesses() {
	defer p.wg.Done()
	// The successes channel must be consumed until it is closed.
	for msg := range p.producer.Successes() {
		var startTime time.Time
		if meta, ok := msg.Metadata.(time.Time); ok {
			startTime = meta
		}
		p.spm.ReportProducerMetrics(context.Background(), []*sarama.ProducerMessage{msg}, nil, startTime)
	}
}

func (p *SaramaAsyncProducer) handleErrors() {
	defer p.wg.Done()
	// The errors channel must be consumed until it is closed.
	for producerErr := range p.producer.Errors() {
		var startTime time.Time
		if meta, ok := producerErr.Msg.Metadata.(time.Time); ok {
			startTime = meta
		}
		p.logger.Error("Failed to produce message to Kafka",
			zap.Error(producerErr),
			zap.String("topic", producerErr.Msg.Topic),
			zap.Int32("partition", producerErr.Msg.Partition),
		)
		// The error passed to ReportProducerMetrics is a single error, not ProducerErrors.
		// The function handles this.
		p.spm.ReportProducerMetrics(context.Background(), []*sarama.ProducerMessage{producerErr.Msg}, producerErr, startTime)
	}
}

// ExportData sends multiple messages to the Kafka broker using the underlying producer.
func (p *SaramaAsyncProducer) ExportData(ctx context.Context, msgs Messages) error {
	messages := makeSaramaMessages(msgs)
	setMessageHeaders(ctx, messages, p.metadataKeys,
		func(key string, value []byte) sarama.RecordHeader {
			return sarama.RecordHeader{Key: []byte(key), Value: value}
		},
		func(m *sarama.ProducerMessage) []sarama.RecordHeader { return m.Headers },
		func(m *sarama.ProducerMessage, h []sarama.RecordHeader) { m.Headers = h },
	)
	for _, msg := range messages {
		msg.Metadata = time.Now()
		p.producer.Input() <- msg
	}
	return nil
}

// Close shuts down the producer and flushes any remaining messages.
func (p *SaramaAsyncProducer) Close() error {
	p.producer.AsyncClose()
	p.wg.Wait()
	return nil
}

func makeSaramaMessages(messages Messages) []*sarama.ProducerMessage {
	msgs := make([]*sarama.ProducerMessage, 0, messages.Count)
	for _, msg := range messages.TopicMessages {
		for _, message := range msg.Messages {
			msg := &sarama.ProducerMessage{Topic: msg.Topic}
			if message.Key != nil {
				msg.Key = sarama.ByteEncoder(message.Key)
			}
			if message.Value != nil {
				msg.Value = sarama.ByteEncoder(message.Value)
			}
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

type kafkaErrors struct {
	count int
	err   string
}

func (ke kafkaErrors) Error() string {
	return fmt.Sprintf("Failed to deliver %d messages due to %s", ke.count, ke.err)
}

func wrapKafkaProducerError(err error) error {
	var prodErr sarama.ProducerErrors
	if !errors.As(err, &prodErr) || len(prodErr) == 0 {
		return err
	}
	var areConfigErrs bool
	var confErr sarama.ConfigurationError
	for _, producerErr := range prodErr {
		if areConfigErrs = errors.As(producerErr.Err, &confErr); !areConfigErrs {
			break
		}
	}
	if areConfigErrs {
		return consumererror.NewPermanent(confErr)
	}

	return kafkaErrors{len(prodErr), prodErr[0].Err.Error()}
}
