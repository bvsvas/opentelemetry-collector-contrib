// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"

import (
	"context"
	"fmt"
	"iter"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/kafkaclient"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/marshaler"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/topic"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

const franzGoClientFeatureGateName = "exporter.kafkaexporter.UseFranzGo"

// franzGoClientFeatureGate is a feature gate that controls whether the Kafka exporter
// uses the franz-go client or the Sarama client. When enabled, the Kafka exporter
// will use the franz-go client, which is more performant and has better support for
// modern Kafka features.
var franzGoClientFeatureGate = featuregate.GlobalRegistry().MustRegister(
	franzGoClientFeatureGateName, featuregate.StageAlpha,
	featuregate.WithRegisterDescription("When enabled, the Kafka exporter will use the franz-go client to produce messages to Kafka."),
	featuregate.WithRegisterFromVersion("v0.128.0"),
)

// producer is an interface that abstracts the Kafka producer operations
// to allow for different implementations (e.g., Sarama, franz-go)
type producer interface {
	// ExportData sends a batch of messages to Kafka
	ExportData(ctx context.Context, messages kafkaclient.Messages) error
	// Close shuts down the producer
	Close() error
}

type messenger[T any] interface {
	// partitionData returns an iterator that yields key-value pairs
	// where the key is the partition key, and the value is the pdata
	// type (plog.Logs, etc.)
	partitionData(T) iter.Seq2[[]byte, T]

	// marshalData marshals a pdata type into one or more messages.
	marshalData(T) ([]marshaler.Message, error)

	// getTopic returns the topic name for the given context and data.
	getTopic(context.Context, T) string
}

type kafkaExporter[T any] struct {
	cfg          Config
	set          exporter.Settings
	tb           *metadata.TelemetryBuilder
	logger       *zap.Logger
	newMessenger func(host component.Host) (messenger[T], error)
	messenger    messenger[T]
	producer     producer
}

func newKafkaExporter[T any](
	config Config,
	set exporter.Settings,
	newMessenger func(component.Host) (messenger[T], error),
) *kafkaExporter[T] {
	return &kafkaExporter[T]{
		cfg:          config,
		set:          set,
		logger:       set.Logger,
		newMessenger: newMessenger,
	}
}

func (e *kafkaExporter[T]) Start(ctx context.Context, host component.Host) (err error) {
	tb, err := metadata.NewTelemetryBuilder(e.set.TelemetrySettings)
	if err != nil {
		return err
	}
	e.tb = tb

	if e.messenger, err = e.newMessenger(host); err != nil {
		return err
	}

	fmt.Println("$$$$$ TEST:AsyncProducer e.cfg.Async.Enabled=", e.cfg.Async.Enabled)
	if franzGoClientFeatureGate.IsEnabled() {
		// The kgo.Client can be used for both sync and async producing.
		// The distinction is made by which wrapper we use.
		kgoClient, ferr := kafka.NewFranzProducer(
			ctx,
			e.cfg.ClientConfig,
			e.cfg.Producer,
			e.cfg.TimeoutSettings.Timeout,
			e.logger,
			kgo.WithHooks(kafkaclient.NewFranzProducerMetrics(tb)),
		)
		if ferr != nil {
			return ferr
		}
		if e.cfg.Async.Enabled {
			fmt.Println("$$$$$ TEST:FranzAsyncProducer $$$$$$")
			e.producer = kafkaclient.NewFranzAsyncProducer(
				kgoClient,
				e.logger,
				e.cfg.IncludeMetadataKeys,
			)
		} else {
			e.producer = kafkaclient.NewFranzSyncProducer(kgoClient,
				e.cfg.IncludeMetadataKeys,
			)
		}
		return nil
	}
	if e.cfg.Async.Enabled {
		producer, err := kafka.NewSaramaAsyncProducer(ctx, e.cfg.ClientConfig,
			e.cfg.Producer, e.cfg.TimeoutSettings.Timeout,
		)
		if err != nil {
			return err
		}
		e.producer = kafkaclient.NewSaramaAsyncProducer(
			producer,
			e.logger,
			e.cfg.IncludeMetadataKeys,
			kafkaclient.NewSaramaProducerMetrics(e.tb),
		)
	} else {
		producer, err := kafka.NewSaramaSyncProducer(ctx, e.cfg.ClientConfig,
			e.cfg.Producer, e.cfg.TimeoutSettings.Timeout,
		)
		if err != nil {
			return err
		}
		e.producer = kafkaclient.NewSaramaSyncProducer(
			producer,
			kafkaclient.NewSaramaProducerMetrics(tb),
			e.cfg.IncludeMetadataKeys,
		)
	}
	return nil
}

func (e *kafkaExporter[T]) Close(context.Context) (err error) {
	if e.producer == nil {
		return nil
	}
	err = e.producer.Close()
	e.producer = nil
	if e.tb != nil {
		e.tb.Shutdown()
		e.tb = nil
	}
	return err
}

func (e *kafkaExporter[T]) exportData(ctx context.Context, data T) error {
	var m kafkaclient.Messages
	for key, data := range e.messenger.partitionData(data) {
		partitionMessages, err := e.messenger.marshalData(data)
		if err != nil {
			return consumererror.NewPermanent(err)
		}
		for i := range partitionMessages {
			// Marshalers may set the Key, so don't override
			// if it's set and we're not partitioning here.
			if key != nil {
				partitionMessages[i].Key = key
			}
		}
		m.Count += len(partitionMessages)
		m.TopicMessages = append(m.TopicMessages, kafkaclient.TopicMessages{
			Topic:    e.messenger.getTopic(ctx, data),
			Messages: partitionMessages,
		})
	}
	return e.producer.ExportData(ctx, m)
}

func newTracesExporter(config Config, set exporter.Settings) *kafkaExporter[ptrace.Traces] {
	// Jaeger encodings do their own partitioning, so disable trace ID
	// partitioning when they are configured.
	switch config.Traces.Encoding {
	case "jaeger_proto", "jaeger_json":
		config.PartitionTracesByID = false
	}
	return newKafkaExporter[ptrace.Traces](config, set, func(host component.Host) (messenger[ptrace.Traces], error) {
		marshaler, err := getTracesMarshaler(config.Traces.Encoding, host)
		if err != nil {
			return nil, err
		}
		return &kafkaTracesMessenger{
			config:    config,
			marshaler: marshaler,
		}, nil
	})
}

type kafkaTracesMessenger struct {
	config    Config
	marshaler marshaler.TracesMarshaler
}

func (e *kafkaTracesMessenger) marshalData(td ptrace.Traces) ([]marshaler.Message, error) {
	return e.marshaler.MarshalTraces(td)
}

func (e *kafkaTracesMessenger) getTopic(ctx context.Context, td ptrace.Traces) string {
	return getTopic(ctx, e.config.Traces, e.config.TopicFromAttribute, td.ResourceSpans())
}

func (e *kafkaTracesMessenger) partitionData(td ptrace.Traces) iter.Seq2[[]byte, ptrace.Traces] {
	return func(yield func([]byte, ptrace.Traces) bool) {
		if !e.config.PartitionTracesByID {
			yield(nil, td)
			return
		}
		for _, td := range batchpersignal.SplitTraces(td) {
			// Note that batchpersignal.SplitTraces guarantees that each trace
			// has exactly one trace, and by implication, at least one span.
			key := []byte(traceutil.TraceIDToHexOrEmptyString(
				td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID(),
			))
			if !yield(key, td) {
				return
			}
		}
	}
}

func newLogsExporter(config Config, set exporter.Settings) *kafkaExporter[plog.Logs] {
	return newKafkaExporter[plog.Logs](config, set, func(host component.Host) (messenger[plog.Logs], error) {
		marshaler, err := getLogsMarshaler(config.Logs.Encoding, host)
		if err != nil {
			return nil, err
		}
		return &kafkaLogsMessenger{
			config:    config,
			marshaler: marshaler,
		}, nil
	})
}

type kafkaLogsMessenger struct {
	config    Config
	marshaler marshaler.LogsMarshaler
}

func (e *kafkaLogsMessenger) marshalData(ld plog.Logs) ([]marshaler.Message, error) {
	return e.marshaler.MarshalLogs(ld)
}

func (e *kafkaLogsMessenger) getTopic(ctx context.Context, ld plog.Logs) string {
	return getTopic(ctx, e.config.Logs, e.config.TopicFromAttribute, ld.ResourceLogs())
}

func (e *kafkaLogsMessenger) partitionData(ld plog.Logs) iter.Seq2[[]byte, plog.Logs] {
	return func(yield func([]byte, plog.Logs) bool) {
		if !e.config.PartitionLogsByResourceAttributes {
			yield(nil, ld)
			return
		}
		for _, resourceLogs := range ld.ResourceLogs().All() {
			hash := pdatautil.MapHash(resourceLogs.Resource().Attributes())
			newLogs := plog.NewLogs()
			resourceLogs.CopyTo(newLogs.ResourceLogs().AppendEmpty())
			if !yield(hash[:], newLogs) {
				return
			}
		}
	}
}

func newMetricsExporter(config Config, set exporter.Settings) *kafkaExporter[pmetric.Metrics] {
	return newKafkaExporter[pmetric.Metrics](config, set, func(host component.Host) (messenger[pmetric.Metrics], error) {
		marshaler, err := getMetricsMarshaler(config.Metrics.Encoding, host)
		if err != nil {
			return nil, err
		}
		return newKafkaMetricsMessenger(config, marshaler), nil
	})
}

// routingStrategy defines the partitioning strategy for metrics
type routingStrategy int

const (
	routingStrategyNone routingStrategy = iota
	routingStrategyResource
	routingStrategyResourceAndMetric
)

type kafkaMetricsMessenger struct {
	config          Config
	marshaler       marshaler.MetricsMarshaler
	routingStrategy routingStrategy // Cached routing strategy to avoid string comparison on every call
}

// newKafkaMetricsMessenger creates a new kafkaMetricsMessenger with properly initialized routing strategy
func newKafkaMetricsMessenger(config Config, marshaler marshaler.MetricsMarshaler) *kafkaMetricsMessenger {
	// Determine and cache routing strategy to avoid string comparison on every call
	strategy := routingStrategyNone
	if config.PartitionMetricsByResourceAttributes {
		routingKey := config.PartitionMetricsRoutingKey
		if routingKey == "" {
			routingKey = "resource" // default
		}
		switch routingKey {
		case "resource_and_metric":
			strategy = routingStrategyResourceAndMetric
		default: // "resource"
			strategy = routingStrategyResource
		}
	}

	return &kafkaMetricsMessenger{
		config:          config,
		marshaler:       marshaler,
		routingStrategy: strategy,
	}
}

func (e *kafkaMetricsMessenger) marshalData(md pmetric.Metrics) ([]marshaler.Message, error) {
	return e.marshaler.MarshalMetrics(md)
}

func (e *kafkaMetricsMessenger) getTopic(ctx context.Context, md pmetric.Metrics) string {
	return getTopic(ctx, e.config.Metrics, e.config.TopicFromAttribute, md.ResourceMetrics())
}

func (e *kafkaMetricsMessenger) partitionData(md pmetric.Metrics) iter.Seq2[[]byte, pmetric.Metrics] {
	return func(yield func([]byte, pmetric.Metrics) bool) {
		// Use cached routing strategy to avoid string comparison on every call
		switch e.routingStrategy {
		case routingStrategyNone:
			// No partitioning: all metrics go to default partition
			yield(nil, md)
		case routingStrategyResourceAndMetric:
			e.partitionByResourceAndMetricBatched(md, yield)
		case routingStrategyResource:
			e.partitionByResource(md, yield)
		}
	}
}

// partitionByResource partitions metrics by resource attributes only (current behavior)
func (e *kafkaMetricsMessenger) partitionByResource(md pmetric.Metrics, yield func([]byte, pmetric.Metrics) bool) {
	for _, resourceMetrics := range md.ResourceMetrics().All() {
		// Hash resource attributes
		hash := pdatautil.MapHash(resourceMetrics.Resource().Attributes())

		// Create new metrics with this resource
		newMetrics := pmetric.NewMetrics()
		resourceMetrics.CopyTo(newMetrics.ResourceMetrics().AppendEmpty())

		if !yield(hash[:], newMetrics) {
			return
		}
	}
}

// partitionByResourceAndMetricBatched partitions metrics by resource attributes + metric name
// with intelligent batching optimization.
//
// STRATEGY:
// This function batches metrics by RESOURCE (keeping all metrics from same source together),
// but calculates partition keys using (resource + metric_name) for load distribution.
//
// This provides:
//  1. HOT PARTITION MITIGATION: Each metric from a hot source can go to a different partition
//  2. EFFICIENT BATCHING: All metrics from same source stay in one Kafka message
//  3. CONSUMER EFFICIENCY: Fewer, larger messages instead of many tiny messages
//
// EXAMPLE with 100 sources × 50 metrics:
//   - resource-only: 100 messages, each with 50 metrics → 100 Kafka partitions
//   - resource_and_metric (old): 5,000 messages, each with 1 metric → 5,000 Kafka partitions (BAD!)
//   - resource_and_metric (batched): 100 messages, each with 50 metrics → uses composite keys for smart distribution
//
// HOW IT WORKS:
// 1. Group metrics by resource (creates ~100 batches for 100 sources)
// 2. For each batch, calculate a composite partition key using ALL metric names
// 3. This distributes hot sources across partitions while keeping messages batched
//
// Performance characteristics:
//   - Time complexity: O(R × S × M) for iteration + O(R) for hashing
//   - Space complexity: O(R) where R = number of unique resources
//   - Hash computation: ~100-200ns per resource batch
//   - Message count: Same as resource-only partitioning (efficient!)
//
// Parameters:
//   - md: The metrics data to partition
//   - yield: Callback function to yield (hash, metrics) pairs
//
// The function yields one (hash, metrics) pair for each unique resource, where:
//   - hash: xxHash128(resource_attributes + all_metric_names_sorted)
//   - metrics: pmetric.Metrics containing all metrics from that resource (batched)
func (e *kafkaMetricsMessenger) partitionByResourceAndMetricBatched(md pmetric.Metrics, yield func([]byte, pmetric.Metrics) bool) {
	// Batch metrics by resource (not by resource+metric)
	// This keeps message count low while still enabling smart partition distribution
	resourceBatches := make(map[[16]byte]*resourceMetricBatch)

	for _, resourceMetrics := range md.ResourceMetrics().All() {
		resource := resourceMetrics.Resource()

		// Hash resource attributes to group metrics from same source
		resourceHash := pdatautil.MapHash(resource.Attributes())

		// Get or create batch for this resource
		batch, exists := resourceBatches[resourceHash]
		if !exists {
			// Create new batch with optimizations:
			// 1. Cache resource hash to avoid recalculation
			// 2. Cache ResourceMetrics reference for O(1) access
			metrics := pmetric.NewMetrics()
			rm := metrics.ResourceMetrics().AppendEmpty()
			resource.CopyTo(rm.Resource())

			batch = &resourceMetricBatch{
				metrics:         metrics,
				resource:        resource,
				resourceHash:    resourceHash,           // OPTIMIZATION: Cache hash
				resourceMetrics: rm,                     // OPTIMIZATION: Cache ResourceMetrics reference
				metricNames:     make([]string, 0, 150), // Preallocate for common case
			}
			resourceBatches[resourceHash] = batch
		}

		// Add all scope metrics from this resource
		for _, scopeMetrics := range resourceMetrics.ScopeMetrics().All() {
			// Collect metric names for composite hash
			for _, metric := range scopeMetrics.Metrics().All() {
				batch.metricNames = append(batch.metricNames, metric.Name())
			}

			// Add entire scope to batch (includes ALL metrics in the ScopeMetrics array)
			batch.addScopeMetrics(resource, scopeMetrics)
		}
	}

	// Yield all resource batches with composite partition keys
	for _, batch := range resourceBatches {
		// Calculate composite partition key: resource + all metric names
		// This distributes hot sources across partitions based on their metric mix
		partitionKey := batch.calculatePartitionKey()
		if !yield(partitionKey, batch.metrics) {
			return
		}
	}
}

// resourceMetricBatch accumulates all metrics from a single resource
// for efficient batching while still enabling composite partition keys
type resourceMetricBatch struct {
	metrics         pmetric.Metrics
	resource        pcommon.Resource
	resourceHash    [16]byte                // Cached hash of resource attributes (optimization)
	resourceMetrics pmetric.ResourceMetrics // Cached ResourceMetrics reference (optimization)
	metricNames     []string                // Collected metric names for composite hash calculation

	// Capacity tracking for observability and debugging
	totalMetrics int // Total number of metrics added to this batch
	totalScopes  int // Total number of scopes added to this batch
}

// addScopeMetrics adds an entire ScopeMetrics to the batch.
//
// CRITICAL: This method copies the ENTIRE ScopeMetrics structure, which includes:
//   - ALL metrics in the Metrics array (no metrics are skipped)
//   - The Scope (InstrumentationScope) information
//   - The SchemaUrl
//
// DATA INTEGRITY GUARANTEE:
//   - Zero data loss: ALL metrics from the input ScopeMetrics are preserved
//   - Zero duplication: Each metric is copied exactly once
//   - Deep copy: scopeMetrics.CopyTo() performs a complete deep copy of all fields
//
// OPTIMIZATION: Uses cached ResourceMetrics reference for O(1) access instead of O(N) iteration
func (b *resourceMetricBatch) addScopeMetrics(resource pcommon.Resource, scopeMetrics pmetric.ScopeMetrics) {
	// Track the number of metrics in this scope BEFORE copying
	metricsInScope := scopeMetrics.Metrics().Len()

	// Copy the entire ScopeMetrics (includes ALL metrics in the Metrics array)
	// This is a deep copy operation that preserves all data
	scopeMetrics.CopyTo(b.resourceMetrics.ScopeMetrics().AppendEmpty())

	// Update capacity tracking for observability
	b.totalScopes++
	b.totalMetrics += metricsInScope
}

// calculatePartitionKey creates a composite hash from resource attributes + metric names
// This enables hot partition mitigation by distributing sources based on their metric mix
func (b *resourceMetricBatch) calculatePartitionKey() []byte {
	// Sort metric names for deterministic hashing
	// (Important: same metrics in different order should hash to same value)
	sort.Strings(b.metricNames)

	// Create composite hash: resource attributes + sorted metric names
	hashOptions := []pdatautil.HashOption{
		pdatautil.WithMap(b.resource.Attributes()),
	}

	// Add each unique metric name to the hash
	// Deduplicate while building hash
	seen := make(map[string]bool)
	for _, name := range b.metricNames {
		if !seen[name] {
			hashOptions = append(hashOptions, pdatautil.WithString(name))
			seen[name] = true
		}
	}

	hash := pdatautil.Hash(hashOptions...)
	return hash[:]
}

func newProfilesExporter(config Config, set exporter.Settings) *kafkaExporter[pprofile.Profiles] {
	return newKafkaExporter(config, set, func(host component.Host) (messenger[pprofile.Profiles], error) {
		marshaler, err := getProfilesMarshaler(config.Profiles.Encoding, host)
		if err != nil {
			return nil, err
		}
		return &kafkaProfilesMessenger{
			config:    config,
			marshaler: marshaler,
		}, nil
	})
}

type kafkaProfilesMessenger struct {
	config    Config
	marshaler marshaler.ProfilesMarshaler
}

func (e *kafkaProfilesMessenger) marshalData(ld pprofile.Profiles) ([]marshaler.Message, error) {
	return e.marshaler.MarshalProfiles(ld)
}

func (e *kafkaProfilesMessenger) getTopic(ctx context.Context, ld pprofile.Profiles) string {
	return getTopic(ctx, e.config.Profiles, e.config.TopicFromAttribute, ld.ResourceProfiles())
}

func (*kafkaProfilesMessenger) partitionData(ld pprofile.Profiles) iter.Seq2[[]byte, pprofile.Profiles] {
	return func(yield func([]byte, pprofile.Profiles) bool) {
		yield(nil, ld)
	}
}

type resourceSlice[T any] interface {
	Len() int
	At(int) T
}

type resource interface {
	Resource() pcommon.Resource
}

func getTopic[T resource](ctx context.Context,
	signalCfg SignalConfig,
	topicFromAttribute string,
	resources resourceSlice[T],
) string {
	if k := signalCfg.TopicFromMetadataKey; k != "" {
		if topic := client.FromContext(ctx).Metadata.Get(k); len(topic) > 0 {
			return topic[0]
		}
	}
	if topicFromAttribute != "" {
		for i := 0; i < resources.Len(); i++ {
			rv, ok := resources.At(i).Resource().Attributes().Get(topicFromAttribute)
			if ok && rv.Str() != "" {
				return rv.Str()
			}
		}
	}
	if topic, ok := topic.FromContext(ctx); ok {
		return topic
	}
	return signalCfg.Topic
}
