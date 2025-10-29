// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/kafkaclient"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka/kafkatest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/topic"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
)

func TestTracesPusher(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
		func(msg *sarama.ProducerMessage) error {
			if msg.Topic != "otlp_spans" {
				return fmt.Errorf(`expected topic "otlp_spans", got %q`, msg.Topic)
			}
			return nil
		},
	)

	err := exp.exportData(context.Background(), testdata.GenerateTraces(2))
	require.NoError(t, err)
}

func TestTracesPusher_attr(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.TopicFromAttribute = "kafka_topic"
	exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
	producer.ExpectSendMessageAndSucceed()

	err := exp.exportData(context.Background(), testdata.GenerateTraces(2))
	require.NoError(t, err)
}

func TestTracesPusher_ctx(t *testing.T) {
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageAndSucceed()

		err := exp.exportData(topic.WithTopic(context.Background(), "my_topic"), testdata.GenerateTraces(2))
		require.NoError(t, err)
	})
	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-tenant-id", "x-request-ids"}
		exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Equal(t, []sarama.RecordHeader{
				{Key: []byte("x-tenant-id"), Value: []byte("my_tenant_id")},
				{Key: []byte("x-request-ids"), Value: []byte("987654321")},
				{Key: []byte("x-request-ids"), Value: []byte("0187262")},
			}, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"987654321", "0187262"},
				"discarded-meta": {"my-meta"}, // This will be ignored.
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateTraces(10))
		require.NoError(t, err)
	})
	t.Run("WithMetadataDisabled", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Nil(t, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"123456789", "0187262"},
				"discarded-meta": {"my-meta"},
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateTraces(5))
		require.NoError(t, err)
	})
}

func TestTracesPusher_attr_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))

	config := createDefaultConfig().(*Config)
	attributeKey := "my_custom_topic_key_traces"
	expectedTopicFromAttribute := "topic_from_traces_attr_kgo"
	config.TopicFromAttribute = attributeKey

	exp, fakeCluster := newKgoMockTracesExporter(t, *config,
		componenttest.NewNopHost(), expectedTopicFromAttribute,
	)

	traces := testdata.GenerateTraces(1)
	traces.ResourceSpans().At(0).Resource().Attributes().PutStr(attributeKey, expectedTopicFromAttribute)

	err := exp.exportData(context.Background(), traces)
	require.NoError(t, err)

	records := fetchKgoRecords(t,
		fakeCluster.ListenAddrs(), expectedTopicFromAttribute,
	)
	fakeCluster.Close()

	require.Len(t, records, 1, "expected one message to be produced, got %d", len(records))
	record := records[0]
	assert.Equal(t, expectedTopicFromAttribute, record.Topic, "message topic mismatch")

	assert.NotEmpty(t, record.Value)
	assert.Empty(t, record.Headers, "expected no headers for this test case")
	assert.Nil(t, record.Key, "expected nil key for this test case")
}

func TestTracesPusher_ctx_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		expectedTopicFromCtx := "my_kgo_topic_from_ctx"
		exp, fakeCluster := newKgoMockTracesExporter(t, *config,
			componenttest.NewNopHost(), expectedTopicFromCtx,
		)

		ctx := topic.WithTopic(context.Background(), expectedTopicFromCtx)
		traces := testdata.GenerateTraces(2)

		err := exp.exportData(ctx, traces)
		require.NoError(t, err)

		records := fetchKgoRecords(t,
			fakeCluster.ListenAddrs(), expectedTopicFromCtx,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, expectedTopicFromCtx, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)
	})

	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-tenant-id", "x-request-ids"}
		exp, fakeCluster := newKgoMockTracesExporter(t, *config,
			componenttest.NewNopHost(), config.Traces.Topic,
		)

		defaultTopic := config.Traces.Topic // Fallback topic if not overridden
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":   {"my_tenant_id"},
				"x-request-ids": {"987654321", "0187262"},
				"ignored-key":   {"some-value"}, // This should be ignored
			}),
		})
		traces := testdata.GenerateTraces(1)

		err := exp.exportData(ctx, traces)
		require.NoError(t, err)

		records := fetchKgoRecords(t,
			fakeCluster.ListenAddrs(), defaultTopic,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, defaultTopic, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)
		assert.ElementsMatch(t, []kgo.RecordHeader{
			{Key: "x-tenant-id", Value: []byte("my_tenant_id")},
			{Key: "x-request-ids", Value: []byte("987654321")},
			{Key: "x-request-ids", Value: []byte("0187262")},
		}, record.Headers, "message headers mismatch")
		assert.Nil(t, record.Key, "expected nil key for this test case")
	})
}

func TestTracesPusher_err(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))

	expErr := errors.New("failed to send")
	producer.ExpectSendMessageAndFail(expErr)

	err := exp.exportData(context.Background(), testdata.GenerateTraces(2))
	assert.EqualError(t, err, expErr.Error())
}

func TestTracesPusher_conf_err(t *testing.T) {
	t.Run("should return permanent err on config error", func(t *testing.T) {
		expErr := sarama.ConfigurationError("configuration error")
		prodErrs := sarama.ProducerErrors{
			&sarama.ProducerError{Err: expErr},
		}
		host := extensionsHost{
			component.MustNewID("trace_encoding"): ptraceMarshalerFuncExtension(func(ptrace.Traces) ([]byte, error) {
				return nil, prodErrs
			}),
		}
		config := createDefaultConfig().(*Config)
		config.Traces.Encoding = "trace_encoding"
		exp, _ := newMockTracesExporter(t, *config, host, exportertest.NewNopSettings(metadata.Type))

		err := exp.exportData(context.Background(), testdata.GenerateTraces(2))

		assert.True(t, consumererror.IsPermanent(err))
	})
}

func TestTracesPusher_marshal_error(t *testing.T) {
	marshalErr := errors.New("failed to marshal")
	host := extensionsHost{
		component.MustNewID("trace_encoding"): ptraceMarshalerFuncExtension(func(ptrace.Traces) ([]byte, error) {
			return nil, marshalErr
		}),
	}
	config := createDefaultConfig().(*Config)
	config.Traces.Encoding = "trace_encoding"
	exp, _ := newMockTracesExporter(t, *config, host, exportertest.NewNopSettings(metadata.Type))

	err := exp.exportData(context.Background(), testdata.GenerateTraces(2))
	assert.ErrorContains(t, err, marshalErr.Error())
}

func TestTracesPusher_partitioning(t *testing.T) {
	input := ptrace.NewTraces()
	resourceSpans := input.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	traceID1 := pcommon.TraceID{1}
	traceID2 := pcommon.TraceID{2}
	span1 := scopeSpans.Spans().AppendEmpty()
	span1.SetTraceID(traceID1)
	span2 := scopeSpans.Spans().AppendEmpty()
	span2.SetTraceID(traceID1)
	span3 := scopeSpans.Spans().AppendEmpty()
	span3.SetTraceID(traceID2)
	span4 := scopeSpans.Spans().AppendEmpty()
	span4.SetTraceID(traceID2)

	t.Run("default_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
			func(msg *sarama.ProducerMessage) error {
				if msg.Key != nil {
					return errors.New("message key should be nil")
				}
				return nil
			},
		)

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)
	})
	t.Run("jaeger_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.Traces.Encoding = "jaeger_json"
		exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))

		// Jaeger encodings produce one message per span,
		// and each one will have the trace ID as the key.
		var keys [][]byte
		for i := 0; i < 4; i++ {
			producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
				func(msg *sarama.ProducerMessage) error {
					key, err := msg.Key.Encode()
					require.NoError(t, err)
					keys = append(keys, key)
					return nil
				},
			)
		}

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)
		require.Len(t, keys, 4)
		require.ElementsMatch(t, [][]byte{
			[]byte(traceID1.String()),
			[]byte(traceID1.String()),
			[]byte(traceID2.String()),
			[]byte(traceID2.String()),
		}, keys)
	})
	t.Run("trace_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.PartitionTracesByID = true
		exp, producer := newMockTracesExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))

		// We should get one message per ResourceSpans,
		// even if they have the same service name.
		var keys [][]byte
		var traces []ptrace.Traces
		for i := 0; i < 2; i++ {
			producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
				func(msg *sarama.ProducerMessage) error {
					value, err := msg.Value.Encode()
					require.NoError(t, err)

					output, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(value)
					require.NoError(t, err)
					traces = append(traces, output)

					key, err := msg.Key.Encode()
					require.NoError(t, err)
					keys = append(keys, key)
					return nil
				},
			)
		}

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)

		expected := ptrace.NewTraces()
		scopeSpans1 := expected.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
		span1.CopyTo(scopeSpans1.Spans().AppendEmpty())
		span2.CopyTo(scopeSpans1.Spans().AppendEmpty())
		scopeSpans2 := expected.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty()
		span3.CopyTo(scopeSpans2.Spans().AppendEmpty())
		span4.CopyTo(scopeSpans2.Spans().AppendEmpty())

		// Combine trace spans so we can compare ignoring order.
		require.Len(t, traces, 2)
		combined := traces[0]
		for _, rs := range traces[1].ResourceSpans().All() {
			rs.CopyTo(combined.ResourceSpans().AppendEmpty())
		}
		assert.NoError(t, ptracetest.CompareTraces(
			expected, combined,
			ptracetest.IgnoreResourceSpansOrder(),
			ptracetest.IgnoreScopeSpansOrder(),
			ptracetest.IgnoreSpansOrder(),
		))

		require.Len(t, keys, 2)
		require.ElementsMatch(t, [][]byte{
			[]byte(traceID1.String()),
			[]byte(traceID2.String()),
		}, keys)
	})
}

func TestMetricsDataPusher(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
		func(msg *sarama.ProducerMessage) error {
			if msg.Topic != "otlp_metrics" {
				return fmt.Errorf(`expected topic "otlp_metrics", got %q`, msg.Topic)
			}
			return nil
		},
	)

	err := exp.exportData(context.Background(), testdata.GenerateMetrics(2))
	require.NoError(t, err)
}

func TestMetricsDataPusher_attr(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.TopicFromAttribute = "kafka_topic"
	exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
	producer.ExpectSendMessageAndSucceed()

	err := exp.exportData(context.Background(), testdata.GenerateMetrics(2))
	require.NoError(t, err)
}

func TestMetricsDataPusher_ctx(t *testing.T) {
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageAndSucceed()

		err := exp.exportData(topic.WithTopic(context.Background(), "my_topic"), testdata.GenerateMetrics(2))
		require.NoError(t, err)
	})
	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-tenant-id", "x-request-ids"}
		exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Equal(t, []sarama.RecordHeader{
				{Key: []byte("x-tenant-id"), Value: []byte("my_tenant_id")},
				{Key: []byte("x-request-ids"), Value: []byte("123456789")},
				{Key: []byte("x-request-ids"), Value: []byte("123141")},
			}, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"123456789", "123141"},
				"discarded-meta": {"my-meta"}, // This will be ignored.
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateMetrics(10))
		require.NoError(t, err)
	})
	t.Run("WithMetadataDisabled", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Nil(t, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"123456789", "123141"},
				"discarded-meta": {"my-meta"},
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateMetrics(5))
		require.NoError(t, err)
	})
}

func TestMetricsPusher_err(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))

	expErr := errors.New("failed to send")
	producer.ExpectSendMessageAndFail(expErr)

	err := exp.exportData(context.Background(), testdata.GenerateMetrics(2))
	assert.EqualError(t, err, expErr.Error())
}

func TestMetricsPusher_success_async(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.Async.Enabled = true

	set := exportertest.NewNopSettings(metadata.Type)
	tel := componenttest.NewTelemetry()
	set.TelemetrySettings = tel.NewTelemetrySettings()
	core, observed := observer.New(zap.ErrorLevel)
	set.Logger = zap.New(core)

	exp, producer := newMockAsyncMetricsExporter(t, *config, componenttest.NewNopHost(), set)

	producer.ExpectInputAndSucceed()

	// The export should return no error.
	err := exp.exportData(context.Background(), testdata.GenerateMetrics(1))
	require.NoError(t, err)

	// No errors should have been logged.
	assert.Equal(t, 0, observed.Len())

	// Wait for the success to be processed and metrics to be recorded.
	assert.Eventually(t, func() bool {
		m, err := tel.GetMetric("otelcol_kafka_exporter_messages")
		if err != nil {
			return false
		}
		return len(m.Data.(metricdata.Sum[int64]).DataPoints) > 0
	}, time.Second, 10*time.Millisecond)

	// Check for metrics
	m, err := tel.GetMetric("otelcol_kafka_exporter_messages")
	require.NoError(t, err)
	points := m.Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)
	topicVal, ok := points[0].Attributes.Value("topic")
	require.True(t, ok)
	assert.Equal(t, "otlp_metrics", topicVal.AsString())
	outcomeVal, ok := points[0].Attributes.Value("outcome")
	require.True(t, ok)
	assert.Equal(t, "success", outcomeVal.AsString())

	latency, err := tel.GetMetric("otelcol_kafka_exporter_latency")
	require.NoError(t, err)
	latencyPoints := latency.Data.(metricdata.Histogram[int64]).DataPoints
	require.Len(t, latencyPoints, 1)
	assert.Equal(t, uint64(1), latencyPoints[0].Count)
}

func TestMetricsPusher_err_async(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.Async.Enabled = true

	set := exportertest.NewNopSettings(metadata.Type)
	tel := componenttest.NewTelemetry()
	set.TelemetrySettings = tel.NewTelemetrySettings()
	core, observed := observer.New(zap.ErrorLevel)
	set.Logger = zap.New(core)

	exp, producer := newMockAsyncMetricsExporter(t, *config, componenttest.NewNopHost(), set)

	expErr := errors.New("failed to send")
	producer.ExpectInputAndFail(expErr)

	err := exp.exportData(context.Background(), testdata.GenerateMetrics(2))
	assert.NoError(t, err)

	// Wait for the error to be logged.
	require.Eventually(t, func() bool {
		return observed.Len() > 0
	}, time.Second, 10*time.Millisecond)

	log := observed.All()[0]
	assert.Equal(t, "Failed to produce message to Kafka", log.Message)

	errVal, ok := log.ContextMap()["error"]
	require.True(t, ok, "error field not found in log context")

	expectedErrStr := "kafka: Failed to produce message to topic otlp_metrics: failed to send"

	// The type of errVal can be either error or string depending on the zap
	// version and configuration. This handles both to make the test robust.
	var actualErrStr string
	switch e := errVal.(type) {
	case error:
		actualErrStr = e.Error()
	case string:
		actualErrStr = e
	default:
		t.Fatalf("unexpected type for error field: %T", errVal)
	}
	assert.Contains(t, actualErrStr, expectedErrStr)

	// Check for metrics
	m, err := tel.GetMetric("otelcol_kafka_exporter_messages")
	require.NoError(t, err)
	points := m.Data.(metricdata.Sum[int64]).DataPoints
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)
	topicVal, ok := points[0].Attributes.Value("topic")
	require.True(t, ok)
	assert.Equal(t, "otlp_metrics", topicVal.AsString())
	outcomeVal, ok := points[0].Attributes.Value("outcome")
	require.True(t, ok)
	assert.Equal(t, "failure", outcomeVal.AsString())
}

func TestMetricsPusher_conf_err(t *testing.T) {
	t.Run("should return permanent err on config error", func(t *testing.T) {
		expErr := sarama.ConfigurationError("configuration error")
		prodErrs := sarama.ProducerErrors{
			&sarama.ProducerError{Err: expErr},
		}
		host := extensionsHost{
			component.MustNewID("metric_encoding"): ptraceMarshalerFuncExtension(func(ptrace.Traces) ([]byte, error) {
				return nil, prodErrs
			}),
		}
		config := createDefaultConfig().(*Config)
		config.Traces.Encoding = "metric_encoding"
		exp, _ := newMockTracesExporter(t, *config, host, exportertest.NewNopSettings(metadata.Type))

		err := exp.exportData(context.Background(), testdata.GenerateTraces(2))

		assert.True(t, consumererror.IsPermanent(err))
	})
}

func TestMetricsPusher_marshal_error(t *testing.T) {
	marshalErr := errors.New("failed to marshal")
	host := extensionsHost{
		component.MustNewID("metric_encoding"): pmetricMarshalerFuncExtension(func(pmetric.Metrics) ([]byte, error) {
			return nil, marshalErr
		}),
	}
	config := createDefaultConfig().(*Config)
	config.Metrics.Encoding = "metric_encoding"
	exp, _ := newMockMetricsExporter(t, *config, host, exportertest.NewNopSettings(metadata.Type))

	err := exp.exportData(context.Background(), testdata.GenerateMetrics(2))
	assert.ErrorContains(t, err, marshalErr.Error())
}

func TestMetricsPusher_partitioning(t *testing.T) {
	input := pmetric.NewMetrics()
	for _, serviceName := range []string{"service1", "service1", "service2"} {
		resourceMetrics := testdata.GenerateMetrics(1).ResourceMetrics().At(0)
		resourceMetrics.Resource().Attributes().PutStr("service.name", serviceName)
		resourceMetrics.CopyTo(input.ResourceMetrics().AppendEmpty())
	}

	t.Run("default_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
			func(msg *sarama.ProducerMessage) error {
				if msg.Key != nil {
					return errors.New("message key should be nil")
				}
				return nil
			},
		)

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)
	})
	t.Run("resource_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		exp, producer := newMockMetricsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))

		// We should get one message per ResourceMetrics,
		// even if they have the same service name.
		var keys [][]byte
		for i := 0; i < 3; i++ {
			producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
				func(msg *sarama.ProducerMessage) error {
					value, err := msg.Value.Encode()
					require.NoError(t, err)

					output, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(value)
					require.NoError(t, err)

					require.Equal(t, 1, output.ResourceMetrics().Len())
					assert.NoError(t, pmetrictest.CompareResourceMetrics(
						input.ResourceMetrics().At(i),
						output.ResourceMetrics().At(0),
					))

					key, err := msg.Key.Encode()
					require.NoError(t, err)
					keys = append(keys, key)
					return nil
				},
			)
		}

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)

		require.Len(t, keys, 3)
		assert.NotEmpty(t, keys[0])
		assert.Equal(t, keys[0], keys[1])
		assert.NotEqual(t, keys[0], keys[2])
	})
}

func TestMetricsDataPusher_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))
	config := createDefaultConfig().(*Config)

	exp, fakeCluster := newKgoMockMetricsExporter(t, *config,
		componenttest.NewNopHost(), config.Metrics.Topic,
	)

	metrics := testdata.GenerateMetrics(2)
	err := exp.exportData(context.Background(), metrics)
	require.NoError(t, err)

	expectedTopic := config.Metrics.Topic

	records := fetchKgoRecords(t,
		fakeCluster.ListenAddrs(), expectedTopic,
	)
	fakeCluster.Close()

	require.Len(t, records, 1, "expected one message to be produced for metrics batch")
	record := records[0]
	assert.Equal(t, expectedTopic, record.Topic, "message topic mismatch")

	assert.NotEmpty(t, record.Value)

	assert.Empty(t, record.Headers, "expected no headers for default config")
	assert.Nil(t, record.Key, "expected nil key for default config")
}

func TestMetricsDataPusher_attr_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))

	config := createDefaultConfig().(*Config)
	attributeKey := "my_custom_topic_key_metrics"
	expectedTopicFromAttribute := "topic_from_metrics_attr_kgo"
	config.TopicFromAttribute = attributeKey // This applies to all signals if not overridden per signal
	// For metrics specifically, it would be config.Metrics.TopicFromAttribute if that existed,
	// but TopicFromAttribute is a top-level config in the current Config struct for this exporter.

	exp, fakeCluster := newKgoMockMetricsExporter(t, *config,
		componenttest.NewNopHost(), expectedTopicFromAttribute,
	)

	metrics := testdata.GenerateMetrics(1)
	// Add the attribute to the first resource's attributes
	metrics.ResourceMetrics().At(0).Resource().Attributes().PutStr(attributeKey, expectedTopicFromAttribute)

	err := exp.exportData(context.Background(), metrics)
	require.NoError(t, err)

	consumerSeedBrokers := fakeCluster.ListenAddrs()
	records := fetchKgoRecords(t,
		consumerSeedBrokers, expectedTopicFromAttribute,
	)

	require.Len(t, records, 1, "expected one message to be produced")
	record := records[0]
	assert.Equal(t, expectedTopicFromAttribute, record.Topic, "message should be sent to topic from attribute")

	assert.NotEmpty(t, record.Value)
	assert.Empty(t, record.Headers, "expected no headers for this test case")
	assert.Nil(t, record.Key, "expected nil key for this test case")
}

func TestMetricsDataPusher_ctx_Kgo(t *testing.T) {
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		expectedTopicFromCtx := "my_kgo_metrics_topic_from_ctx"
		exp, fakeCluster := newKgoMockMetricsExporter(t, *config,
			componenttest.NewNopHost(), expectedTopicFromCtx,
		)

		ctx := topic.WithTopic(context.Background(), expectedTopicFromCtx)
		metrics := testdata.GenerateMetrics(2)

		err := exp.exportData(ctx, metrics)
		require.NoError(t, err)

		consumerSeedBrokers := fakeCluster.ListenAddrs()
		records := fetchKgoRecords(t,
			consumerSeedBrokers, expectedTopicFromCtx,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, expectedTopicFromCtx, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)
	})

	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-metrics-tenant-id", "x-metrics-req-id"}
		exp, fakeCluster := newKgoMockMetricsExporter(t, *config,
			componenttest.NewNopHost(), config.Metrics.Topic,
		)

		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-metrics-tenant-id": {"metrics_tenant"},
				"x-metrics-req-id":    {"req123", "req456"},
				"ignored-key":         {"some-value"},
			}),
		})
		metrics := testdata.GenerateMetrics(1)

		err := exp.exportData(ctx, metrics)
		require.NoError(t, err)

		consumerSeedBrokers := fakeCluster.ListenAddrs()
		records := fetchKgoRecords(t,
			consumerSeedBrokers, config.Metrics.Topic,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, config.Metrics.Topic, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)

		expectedHeaders := []kgo.RecordHeader{
			{Key: "x-metrics-tenant-id", Value: []byte("metrics_tenant")},
			{Key: "x-metrics-req-id", Value: []byte("req123")},
			{Key: "x-metrics-req-id", Value: []byte("req456")},
		}
		assert.ElementsMatch(t, expectedHeaders, record.Headers, "message headers mismatch")
	})
}

func TestLogsDataPusher(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
		func(msg *sarama.ProducerMessage) error {
			if msg.Topic != "otlp_logs" {
				return fmt.Errorf(`expected topic "otlp_logs", got %q`, msg.Topic)
			}
			return nil
		},
	)

	err := exp.exportData(context.Background(), testdata.GenerateLogs(2))
	require.NoError(t, err)
}

func TestLogsDataPusher_attr(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.TopicFromAttribute = "kafka_topic"
	exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
	producer.ExpectSendMessageAndSucceed()

	err := exp.exportData(context.Background(), testdata.GenerateLogs(2))
	require.NoError(t, err)
}

func TestLogsDataPusher_ctx(t *testing.T) {
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageAndSucceed()

		err := exp.exportData(topic.WithTopic(context.Background(), "my_topic"), testdata.GenerateLogs(2))
		require.NoError(t, err)
	})
	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-tenant-id", "x-request-ids"}
		exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Equal(t, []sarama.RecordHeader{
				{Key: []byte("x-tenant-id"), Value: []byte("my_tenant_id")},
				{Key: []byte("x-request-ids"), Value: []byte("123456789")},
				{Key: []byte("x-request-ids"), Value: []byte("123141")},
			}, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"123456789", "123141"},
				"discarded-meta": {"my-meta"}, // This will be ignored.
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateLogs(10))
		require.NoError(t, err)
	})
	t.Run("WithMetadataDisabled", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Nil(t, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"123456789", "123141"},
				"discarded-meta": {"my-meta"},
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateLogs(5))
		require.NoError(t, err)
	})
}

func TestLogsDataPusher_attr_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))
	config := createDefaultConfig().(*Config)
	attributeKey := "my_custom_topic_key_logs"
	expectedTopicFromAttribute := "topic_from_logs_attr_kgo"
	config.TopicFromAttribute = attributeKey

	exp, fakeCluster := newKgoMockLogsExporter(t, *config,
		componenttest.NewNopHost(), expectedTopicFromAttribute,
	)

	logs := testdata.GenerateLogs(1)
	logs.ResourceLogs().At(0).Resource().Attributes().PutStr(attributeKey, expectedTopicFromAttribute)

	err := exp.exportData(context.Background(), logs)
	require.NoError(t, err)

	records := fetchKgoRecords(t,
		fakeCluster.ListenAddrs(), expectedTopicFromAttribute,
	)
	fakeCluster.Close()

	require.Len(t, records, 1, "expected one message to be produced, got %d", len(records))
	record := records[0]
	assert.Equal(t, expectedTopicFromAttribute, record.Topic, "message topic mismatch")

	assert.NotEmpty(t, record.Value)
	assert.Empty(t, record.Headers, "expected no headers for this test case")
	assert.Nil(t, record.Key, "expected nil key for this test case")
}

func TestLogsDataPusher_ctx_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		expectedTopicFromCtx := "my_kgo_logs_topic_from_ctx"
		exp, fakeCluster := newKgoMockLogsExporter(t, *config,
			componenttest.NewNopHost(), expectedTopicFromCtx,
		)

		ctx := topic.WithTopic(context.Background(), expectedTopicFromCtx)
		logs := testdata.GenerateLogs(2)

		err := exp.exportData(ctx, logs)
		require.NoError(t, err)

		records := fetchKgoRecords(t,
			fakeCluster.ListenAddrs(), expectedTopicFromCtx,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, expectedTopicFromCtx, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)
	})

	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-tenant-id", "x-request-ids"}
		exp, fakeCluster := newKgoMockLogsExporter(t, *config,
			componenttest.NewNopHost(), config.Logs.Topic,
		)

		defaultTopic := config.Logs.Topic // Fallback topic if not overridden
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":   {"my_tenant_id"},
				"x-request-ids": {"987654321", "0187262"},
				"ignored-key":   {"some-value"}, // This should be ignored
			}),
		})
		logs := testdata.GenerateLogs(1)

		err := exp.exportData(ctx, logs)
		require.NoError(t, err)

		records := fetchKgoRecords(t,
			fakeCluster.ListenAddrs(), defaultTopic,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, defaultTopic, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)
		expectedHeaders := []kgo.RecordHeader{
			{Key: "x-tenant-id", Value: []byte("my_tenant_id")},
			{Key: "x-request-ids", Value: []byte("987654321")},
			{Key: "x-request-ids", Value: []byte("0187262")},
		}
		assert.ElementsMatch(t, expectedHeaders, record.Headers, "message headers mismatch")
	})
}

func TestLogsPusher_err(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))

	expErr := errors.New("failed to send")
	producer.ExpectSendMessageAndFail(expErr)

	err := exp.exportData(context.Background(), testdata.GenerateLogs(2))
	assert.EqualError(t, err, expErr.Error())
}

func TestLogsPusher_conf_err(t *testing.T) {
	t.Run("should return permanent err on config error", func(t *testing.T) {
		expErr := sarama.ConfigurationError("configuration error")
		prodErrs := sarama.ProducerErrors{
			&sarama.ProducerError{Err: expErr},
		}
		host := extensionsHost{
			component.MustNewID("log_encoding"): ptraceMarshalerFuncExtension(func(ptrace.Traces) ([]byte, error) {
				return nil, prodErrs
			}),
		}
		config := createDefaultConfig().(*Config)
		config.Traces.Encoding = "log_encoding"
		exp, _ := newMockTracesExporter(t, *config, host, exportertest.NewNopSettings(metadata.Type))

		err := exp.exportData(context.Background(), testdata.GenerateTraces(2))

		assert.True(t, consumererror.IsPermanent(err))
	})
}

func TestLogsPusher_marshal_error(t *testing.T) {
	marshalErr := errors.New("failed to marshal")
	host := extensionsHost{
		component.MustNewID("log_encoding"): plogMarshalerFuncExtension(func(plog.Logs) ([]byte, error) {
			return nil, marshalErr
		}),
	}
	config := createDefaultConfig().(*Config)
	config.Logs.Encoding = "log_encoding"
	exp, _ := newMockLogsExporter(t, *config, host, exportertest.NewNopSettings(metadata.Type))

	err := exp.exportData(context.Background(), testdata.GenerateLogs(2))
	assert.ErrorContains(t, err, marshalErr.Error())
}

func TestLogsPusher_partitioning(t *testing.T) {
	input := plog.NewLogs()
	for _, serviceName := range []string{"service1", "service1", "service2"} {
		resourceLogs := testdata.GenerateLogs(1).ResourceLogs().At(0)
		resourceLogs.Resource().Attributes().PutStr("service.name", serviceName)
		resourceLogs.CopyTo(input.ResourceLogs().AppendEmpty())
	}

	t.Run("default_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
			func(msg *sarama.ProducerMessage) error {
				if msg.Key != nil {
					return errors.New("message key should be nil")
				}
				return nil
			},
		)

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)
	})
	t.Run("resource_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.PartitionLogsByResourceAttributes = true
		exp, producer := newMockLogsExporter(t, *config, componenttest.NewNopHost(), exportertest.NewNopSettings(metadata.Type))

		// We should get one message per ResourceLogs,
		// even if they have the same service name.
		var keys [][]byte
		for i := 0; i < 3; i++ {
			producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
				func(msg *sarama.ProducerMessage) error {
					value, err := msg.Value.Encode()
					require.NoError(t, err)

					output, err := (&plog.ProtoUnmarshaler{}).UnmarshalLogs(value)
					require.NoError(t, err)

					require.Equal(t, 1, output.ResourceLogs().Len())
					assert.NoError(t, plogtest.CompareResourceLogs(
						input.ResourceLogs().At(i),
						output.ResourceLogs().At(0),
					))

					key, err := msg.Key.Encode()
					require.NoError(t, err)
					keys = append(keys, key)
					return nil
				},
			)
		}

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)

		require.Len(t, keys, 3)
		assert.NotEmpty(t, keys[0])
		assert.Equal(t, keys[0], keys[1])
		assert.NotEqual(t, keys[0], keys[2])
	})
}

func TestProfilesPusher(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockProfilesExporter(t, *config, componenttest.NewNopHost())
	producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
		func(msg *sarama.ProducerMessage) error {
			if msg.Topic != "otlp_profiles" {
				return fmt.Errorf(`expected topic "otlp_profiles", got %q`, msg.Topic)
			}
			return nil
		},
	)

	err := exp.exportData(context.Background(), testdata.GenerateProfiles(2))
	require.NoError(t, err)
}

func TestProfilesPusher_attr(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.TopicFromAttribute = "kafka_topic"
	exp, producer := newMockProfilesExporter(t, *config, componenttest.NewNopHost())
	producer.ExpectSendMessageAndSucceed()

	err := exp.exportData(context.Background(), testdata.GenerateProfiles(2))
	require.NoError(t, err)
}

func TestProfilesPusher_ctx(t *testing.T) {
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockProfilesExporter(t, *config, componenttest.NewNopHost())
		producer.ExpectSendMessageAndSucceed()

		err := exp.exportData(topic.WithTopic(context.Background(), "my_topic"), testdata.GenerateProfiles(2))
		require.NoError(t, err)
	})
	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-tenant-id", "x-request-ids"}
		exp, producer := newMockProfilesExporter(t, *config, componenttest.NewNopHost())
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Equal(t, []sarama.RecordHeader{
				{Key: []byte("x-tenant-id"), Value: []byte("my_tenant_id")},
				{Key: []byte("x-request-ids"), Value: []byte("987654321")},
				{Key: []byte("x-request-ids"), Value: []byte("0187262")},
			}, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"987654321", "0187262"},
				"discarded-meta": {"my-meta"}, // This will be ignored.
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateProfiles(10))
		require.NoError(t, err)
	})
	t.Run("WithMetadataDisabled", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockProfilesExporter(t, *config, componenttest.NewNopHost())
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(pm *sarama.ProducerMessage) error {
			assert.Nil(t, pm.Headers)
			return nil
		})
		t.Cleanup(func() {
			require.NoError(t, exp.Close(context.Background()))
		})
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":    {"my_tenant_id"},
				"x-request-ids":  {"123456789", "0187262"},
				"discarded-meta": {"my-meta"},
			}),
		})
		err := exp.exportData(ctx, testdata.GenerateProfiles(5))
		require.NoError(t, err)
	})
}

func TestProfilesPusher_attr_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))

	config := createDefaultConfig().(*Config)
	attributeKey := "my_custom_topic_key_profile"
	expectedTopicFromAttribute := "topic_from_profiles_attr_kgo"
	config.TopicFromAttribute = attributeKey

	exp, fakeCluster := newKgoMockProfilesExporter(t, *config,
		componenttest.NewNopHost(), expectedTopicFromAttribute,
	)

	profiles := testdata.GenerateProfiles(1)
	profiles.ResourceProfiles().At(0).Resource().Attributes().PutStr(attributeKey, expectedTopicFromAttribute)

	err := exp.exportData(context.Background(), profiles)
	require.NoError(t, err)

	records := fetchKgoRecords(t,
		fakeCluster.ListenAddrs(), expectedTopicFromAttribute,
	)
	fakeCluster.Close()

	require.Len(t, records, 1, "expected one message to be produced, got %d", len(records))
	record := records[0]
	assert.Equal(t, expectedTopicFromAttribute, record.Topic, "message topic mismatch")

	assert.NotEmpty(t, record.Value)
	assert.Empty(t, record.Headers, "expected no headers for this test case")
	assert.Nil(t, record.Key, "expected nil key for this test case")
}

func TestProfilesPusher_ctx_Kgo(t *testing.T) {
	require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true))
	defer require.NoError(t, featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false))
	t.Run("WithTopic", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		expectedTopicFromCtx := "my_kgo_topic_from_ctx"
		exp, fakeCluster := newKgoMockProfilesExporter(t, *config,
			componenttest.NewNopHost(), expectedTopicFromCtx,
		)

		ctx := topic.WithTopic(context.Background(), expectedTopicFromCtx)
		profiles := testdata.GenerateProfiles(2)

		err := exp.exportData(ctx, profiles)
		require.NoError(t, err)

		records := fetchKgoRecords(t,
			fakeCluster.ListenAddrs(), expectedTopicFromCtx,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, expectedTopicFromCtx, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)
	})

	t.Run("WithMetadata", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		config.IncludeMetadataKeys = []string{"x-tenant-id", "x-request-ids"}
		exp, fakeCluster := newKgoMockProfilesExporter(t, *config,
			componenttest.NewNopHost(), config.Profiles.Topic,
		)

		defaultTopic := config.Profiles.Topic // Fallback topic if not overridden
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"x-tenant-id":   {"my_tenant_id"},
				"x-request-ids": {"987654321", "0187262"},
				"ignored-key":   {"some-value"}, // This should be ignored
			}),
		})
		profiles := testdata.GenerateProfiles(1)

		err := exp.exportData(ctx, profiles)
		require.NoError(t, err)

		records := fetchKgoRecords(t,
			fakeCluster.ListenAddrs(), defaultTopic,
		)
		require.Len(t, records, 1, "expected one message to be produced")
		record := records[0]
		assert.Equal(t, defaultTopic, record.Topic, "message topic mismatch")
		assert.NotEmpty(t, record.Value)
		assert.ElementsMatch(t, []kgo.RecordHeader{
			{Key: "x-tenant-id", Value: []byte("my_tenant_id")},
			{Key: "x-request-ids", Value: []byte("987654321")},
			{Key: "x-request-ids", Value: []byte("0187262")},
		}, record.Headers, "message headers mismatch")
		assert.Nil(t, record.Key, "expected nil key for this test case")
	})
}

func TestProfilesPusher_err(t *testing.T) {
	config := createDefaultConfig().(*Config)
	exp, producer := newMockProfilesExporter(t, *config, componenttest.NewNopHost())

	expErr := errors.New("failed to send")
	producer.ExpectSendMessageAndFail(expErr)

	err := exp.exportData(context.Background(), testdata.GenerateProfiles(2))
	assert.EqualError(t, err, expErr.Error())
}

func TestProfilesPusher_conf_err(t *testing.T) {
	t.Run("should return permanent err on config error", func(t *testing.T) {
		expErr := sarama.ConfigurationError("configuration error")
		prodErrs := sarama.ProducerErrors{
			&sarama.ProducerError{Err: expErr},
		}
		host := extensionsHost{
			component.MustNewID("profile_encoding"): pprofileMarshalerFuncExtension(func(pprofile.Profiles) ([]byte, error) {
				return nil, prodErrs
			}),
		}
		config := createDefaultConfig().(*Config)
		config.Profiles.Encoding = "profile_encoding"
		exp, _ := newMockProfilesExporter(t, *config, host)

		err := exp.exportData(context.Background(), testdata.GenerateProfiles(2))

		assert.True(t, consumererror.IsPermanent(err))
	})
}

func TestProfilesPusher_marshal_error(t *testing.T) {
	marshalErr := errors.New("failed to marshal")
	host := extensionsHost{
		component.MustNewID("profile_encoding"): pprofileMarshalerFuncExtension(func(pprofile.Profiles) ([]byte, error) {
			return nil, marshalErr
		}),
	}
	config := createDefaultConfig().(*Config)
	config.Profiles.Encoding = "profile_encoding"
	exp, _ := newMockProfilesExporter(t, *config, host)

	err := exp.exportData(context.Background(), testdata.GenerateProfiles(2))
	assert.ErrorContains(t, err, marshalErr.Error())
}

func TestProfilesPusher_partitioning(t *testing.T) {
	input := testdata.GenerateProfiles(0)
	for _, serviceName := range []string{"service1", "service1", "service2"} {
		resourceProfiles := testdata.GenerateProfiles(1).ResourceProfiles().At(0)
		resourceProfiles.Resource().Attributes().PutStr("service.name", serviceName)
		resourceProfiles.CopyTo(input.ResourceProfiles().AppendEmpty())
	}

	t.Run("default_partitioning", func(t *testing.T) {
		config := createDefaultConfig().(*Config)
		exp, producer := newMockProfilesExporter(t, *config, componenttest.NewNopHost())
		producer.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(
			func(msg *sarama.ProducerMessage) error {
				if msg.Key != nil {
					return errors.New("message key should be nil")
				}
				return nil
			},
		)

		err := exp.exportData(context.Background(), input)
		require.NoError(t, err)
	})
}

func Test_GetTopic(t *testing.T) {
	tests := []struct {
		name               string
		topicFromAttribute string
		signalCfg          SignalConfig
		ctx                context.Context
		resource           any
		wantTopic          string
	}{
		// topicFromAttribute tests.
		{
			name:               "Valid metric attribute, return topic name",
			topicFromAttribute: "resource-attr",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                topic.WithTopic(context.Background(), "context-topic"),
			resource:           testdata.GenerateMetrics(1).ResourceMetrics(),
			wantTopic:          "resource-attr-val-1",
		},
		{
			name:               "Valid trace attribute, return topic name",
			topicFromAttribute: "resource-attr",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                topic.WithTopic(context.Background(), "context-topic"),
			resource:           testdata.GenerateTraces(1).ResourceSpans(),
			wantTopic:          "resource-attr-val-1",
		},
		{
			name:               "Valid log attribute, return topic name",
			topicFromAttribute: "resource-attr",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                topic.WithTopic(context.Background(), "context-topic"),
			resource:           testdata.GenerateLogs(1).ResourceLogs(),
			wantTopic:          "resource-attr-val-1",
		},
		{
			name:               "Attribute not found",
			topicFromAttribute: "nonexistent_attribute",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                context.Background(),
			resource:           testdata.GenerateMetrics(1).ResourceMetrics(),
			wantTopic:          "defaultTopic",
		},
		// Nonexistent attribute tests.
		{
			name:               "Valid metric context, return topic name",
			topicFromAttribute: "nonexistent_attribute",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                topic.WithTopic(context.Background(), "context-topic"),
			resource:           testdata.GenerateMetrics(1).ResourceMetrics(),
			wantTopic:          "context-topic",
		},
		{
			name:               "Valid trace context, return topic name",
			topicFromAttribute: "nonexistent_attribute",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                topic.WithTopic(context.Background(), "context-topic"),
			resource:           testdata.GenerateTraces(1).ResourceSpans(),
			wantTopic:          "context-topic",
		},
		{
			name:               "Valid log context, return topic name",
			topicFromAttribute: "nonexistent_attribute",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                topic.WithTopic(context.Background(), "context-topic"),
			resource:           testdata.GenerateLogs(1).ResourceLogs(),
			wantTopic:          "context-topic",
		},
		// Generic known failure modes.
		{
			name:               "Attribute not found",
			topicFromAttribute: "nonexistent_attribute",
			signalCfg:          SignalConfig{Topic: "defaultTopic"},
			ctx:                context.Background(),
			resource:           testdata.GenerateMetrics(1).ResourceMetrics(),
			wantTopic:          "defaultTopic",
		},
		{
			name:      "TopicFromAttribute, return default topic",
			ctx:       context.Background(),
			signalCfg: SignalConfig{Topic: "defaultTopic"},
			resource:  testdata.GenerateMetrics(1).ResourceMetrics(),
			wantTopic: "defaultTopic",
		},
		// topicFromMetadata tests.
		{
			name: "Metrics topic from metadata",
			signalCfg: SignalConfig{
				Topic:                "defaultTopic",
				TopicFromMetadataKey: "metrics_topic_metadata",
			},
			ctx: client.NewContext(context.Background(),
				client.Info{Metadata: client.NewMetadata(map[string][]string{
					"metrics_topic_metadata": {"my_metrics_topic"},
				})},
			),
			resource:  testdata.GenerateMetrics(1).ResourceMetrics(),
			wantTopic: "my_metrics_topic",
		},
		{
			name: "Logs topic from metadata",
			signalCfg: SignalConfig{
				Topic:                "defaultTopic",
				TopicFromMetadataKey: "logs_topic_metadata",
			},
			ctx: client.NewContext(context.Background(),
				client.Info{Metadata: client.NewMetadata(map[string][]string{
					"logs_topic_metadata": {"my_logs_topic"},
				})},
			),
			resource:  testdata.GenerateLogs(1).ResourceLogs(),
			wantTopic: "my_logs_topic",
		},
		{
			name: "Traces topic from metadata",
			signalCfg: SignalConfig{
				Topic:                "defaultTopic",
				TopicFromMetadataKey: "traces_topic_metadata",
			},
			ctx: client.NewContext(context.Background(),
				client.Info{Metadata: client.NewMetadata(map[string][]string{
					"traces_topic_metadata": {"my_traces_topic"},
				})},
			),
			resource:  testdata.GenerateTraces(1).ResourceSpans(),
			wantTopic: "my_traces_topic",
		},
		{
			name: "metadata key not found uses default topic",
			signalCfg: SignalConfig{
				Topic:                "defaultTopic",
				TopicFromMetadataKey: "key not found",
			},
			ctx: client.NewContext(context.Background(),
				client.Info{Metadata: client.NewMetadata(map[string][]string{
					"traces_topic_metadata": {"my_traces_topic"},
				})},
			),
			resource:  testdata.GenerateTraces(1).ResourceSpans(),
			wantTopic: "defaultTopic",
		},
	}

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			topic := ""
			switch r := tests[i].resource.(type) {
			case pmetric.ResourceMetricsSlice:
				topic = getTopic(tests[i].ctx, tests[i].signalCfg, tests[i].topicFromAttribute, r)
			case ptrace.ResourceSpansSlice:
				topic = getTopic(tests[i].ctx, tests[i].signalCfg, tests[i].topicFromAttribute, r)
			case plog.ResourceLogsSlice:
				topic = getTopic(tests[i].ctx, tests[i].signalCfg, tests[i].topicFromAttribute, r)
			}
			assert.Equal(t, tests[i].wantTopic, topic)
		})
	}
}

type extensionsHost map[component.ID]component.Component

func (m extensionsHost) GetExtensions() map[component.ID]component.Component {
	return m
}

type ptraceMarshalerFuncExtension func(ptrace.Traces) ([]byte, error)

func (f ptraceMarshalerFuncExtension) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return f(td)
}

func (ptraceMarshalerFuncExtension) Start(context.Context, component.Host) error {
	return nil
}

func (ptraceMarshalerFuncExtension) Shutdown(context.Context) error {
	return nil
}

type pmetricMarshalerFuncExtension func(pmetric.Metrics) ([]byte, error)

func (f pmetricMarshalerFuncExtension) MarshalMetrics(td pmetric.Metrics) ([]byte, error) {
	return f(td)
}

func (pmetricMarshalerFuncExtension) Start(context.Context, component.Host) error {
	return nil
}

func (pmetricMarshalerFuncExtension) Shutdown(context.Context) error {
	return nil
}

type plogMarshalerFuncExtension func(plog.Logs) ([]byte, error)

func (f plogMarshalerFuncExtension) MarshalLogs(td plog.Logs) ([]byte, error) {
	return f(td)
}

func (plogMarshalerFuncExtension) Start(context.Context, component.Host) error {
	return nil
}

func (plogMarshalerFuncExtension) Shutdown(context.Context) error {
	return nil
}

type pprofileMarshalerFuncExtension func(pprofile.Profiles) ([]byte, error)

func (f pprofileMarshalerFuncExtension) MarshalProfiles(td pprofile.Profiles) ([]byte, error) {
	return f(td)
}

func (pprofileMarshalerFuncExtension) Start(context.Context, component.Host) error {
	return nil
}

func (pprofileMarshalerFuncExtension) Shutdown(context.Context) error {
	return nil
}

func newMockTracesExporter(t *testing.T, cfg Config, host component.Host, set exporter.Settings) (*kafkaExporter[ptrace.Traces], *mocks.SyncProducer) {
	exp := newTracesExporter(cfg, set)

	// Fake starting the exporter.
	messenger, err := exp.newMessenger(host)
	require.NoError(t, err)
	exp.messenger = messenger

	// Create a mock producer.
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)
	exp.producer = kafkaclient.NewSaramaSyncProducer(
		producer,
		kafkaclient.NewSaramaProducerMetrics(tb),
		cfg.IncludeMetadataKeys,
	)

	t.Cleanup(func() {
		assert.NoError(t, exp.Close(context.Background()))
	})
	return exp, producer
}

func newMockMetricsExporter(t *testing.T, cfg Config, host component.Host, set exporter.Settings) (*kafkaExporter[pmetric.Metrics], *mocks.SyncProducer) {
	exp := newMetricsExporter(cfg, set)

	// Fake starting the exporter.
	messenger, err := exp.newMessenger(host)
	require.NoError(t, err)
	exp.messenger = messenger

	// Create a mock producer.
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)
	exp.producer = kafkaclient.NewSaramaSyncProducer(
		producer,
		kafkaclient.NewSaramaProducerMetrics(tb),
		cfg.IncludeMetadataKeys,
	)

	t.Cleanup(func() {
		assert.NoError(t, exp.Close(context.Background()))
	})
	return exp, producer
}

func newMockAsyncMetricsExporter(t *testing.T, cfg Config, host component.Host, set exporter.Settings) (*kafkaExporter[pmetric.Metrics], *mocks.AsyncProducer) {
	exp := newMetricsExporter(cfg, set)

	// Fake starting the exporter.
	messenger, err := exp.newMessenger(host)
	require.NoError(t, err)
	exp.messenger = messenger
	exp.logger = set.Logger
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)
	exp.tb = tb

	// Create a mock producer.
	producer := mocks.NewAsyncProducer(t, sarama.NewConfig())
	exp.producer = kafkaclient.NewSaramaAsyncProducer(
		producer,
		set.Logger,
		cfg.IncludeMetadataKeys,
		kafkaclient.NewSaramaProducerMetrics(tb),
	)

	t.Cleanup(func() { _ = exp.Close(context.Background()) })
	return exp, producer
}

func newMockLogsExporter(t *testing.T, cfg Config, host component.Host, set exporter.Settings) (*kafkaExporter[plog.Logs], *mocks.SyncProducer) {
	exp := newLogsExporter(cfg, set)

	// Fake starting the exporter.
	messenger, err := exp.newMessenger(host)
	require.NoError(t, err)
	exp.messenger = messenger

	// Create a mock producer.
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)
	exp.producer = kafkaclient.NewSaramaSyncProducer(
		producer,
		kafkaclient.NewSaramaProducerMetrics(tb),
		cfg.IncludeMetadataKeys,
	)

	t.Cleanup(func() {
		assert.NoError(t, exp.Close(context.Background()))
	})
	return exp, producer
}

func newMockProfilesExporter(t *testing.T, cfg Config, host component.Host) (*kafkaExporter[pprofile.Profiles], *mocks.SyncProducer) {
	set := exportertest.NewNopSettings(metadata.Type)
	exp := newProfilesExporter(cfg, set)

	// Fake starting the exporter.
	messenger, err := exp.newMessenger(host)
	require.NoError(t, err)
	exp.messenger = messenger

	// Create a mock producer.
	producer := mocks.NewSyncProducer(t, sarama.NewConfig())
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)
	exp.producer = kafkaclient.NewSaramaSyncProducer(
		producer,
		kafkaclient.NewSaramaProducerMetrics(tb),
		cfg.IncludeMetadataKeys,
	)

	t.Cleanup(func() {
		assert.NoError(t, exp.Close(context.Background()))
	})
	return exp, producer
}

func newKgoMockLogsExporter(t *testing.T, cfg Config, host component.Host, topics ...string) (*kafkaExporter[plog.Logs], *kfake.Cluster) {
	exp := newLogsExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	cluster := configureExporter(t, exp, cfg, host, topics...)
	return exp, cluster
}

func newKgoMockTracesExporter(t *testing.T, cfg Config, host component.Host, topics ...string) (*kafkaExporter[ptrace.Traces], *kfake.Cluster) {
	exp := newTracesExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	cluster := configureExporter(t, exp, cfg, host, topics...)
	return exp, cluster
}

func newKgoMockMetricsExporter(t *testing.T, cfg Config, host component.Host, topics ...string) (*kafkaExporter[pmetric.Metrics], *kfake.Cluster) {
	exp := newMetricsExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	cluster := configureExporter(t, exp, cfg, host, topics...)
	return exp, cluster
}

func newKgoMockProfilesExporter(t *testing.T, cfg Config, host component.Host, topics ...string) (*kafkaExporter[pprofile.Profiles], *kfake.Cluster) {
	exp := newProfilesExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	cluster := configureExporter(t, exp, cfg, host, topics...)
	return exp, cluster
}

func configureExporter[T any](tb testing.TB,
	exp *kafkaExporter[T], cfg Config, host component.Host, topics ...string,
) *kfake.Cluster {
	cluster, kcfg := kafkatest.NewCluster(tb, kfake.SeedTopics(1, topics...))

	// Create a kgo.Client using the broker addresses from the fake cluster.
	kgoClientOpts := []kgo.Opt{
		kgo.SeedBrokers(kcfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
	}
	client, err := kgo.NewClient(kgoClientOpts...)
	require.NoError(tb, err, "failed to create kgo.Client with fake cluster addresses")

	messenger, err := exp.newMessenger(host) // messenger implements Marshaler[pmetric.Metrics]
	require.NoError(tb, err, "failed to create messenger for metrics")

	exp.messenger = messenger
	exp.producer = kafkaclient.NewFranzSyncProducer(client, cfg.IncludeMetadataKeys)

	tb.Cleanup(func() { assert.NoError(tb, exp.Close(context.Background())) })
	return cluster
}

// fetchKgoRecords polls a franz-go topic for up to 5 seconds and returns all records produced to that topic.
func fetchKgoRecords(tb testing.TB, brokers []string, topic string) []*kgo.Record {
	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("group-id" + topic),
	}
	consumerClient, err := kgo.NewClient(clientOpts...)
	require.NoError(tb, err, "failed to create kgo consumer client")
	defer consumerClient.Close()

	ctx, cancel := context.WithTimeoutCause(context.Background(), 500*time.Millisecond,
		errors.New("No records were received"))
	defer cancel()

	var records []*kgo.Record
	fetches := consumerClient.PollRecords(ctx, 1)
	require.NoError(tb, fetches.Err(), "error polling records")
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})
	return records
}

// TestPartitionByResource tests the default partitioning strategy (by resource only)
func TestPartitionByResource(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource"

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	// Create metrics with multiple resources
	md := pmetric.NewMetrics()

	// Resource 1: service.name=service-a
	rm1 := md.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("service.name", "service-a")
	sm1 := rm1.ScopeMetrics().AppendEmpty()
	m1 := sm1.Metrics().AppendEmpty()
	m1.SetName("metric1")
	m2 := sm1.Metrics().AppendEmpty()
	m2.SetName("metric2")

	// Resource 2: service.name=service-b
	rm2 := md.ResourceMetrics().AppendEmpty()
	rm2.Resource().Attributes().PutStr("service.name", "service-b")
	sm2 := rm2.ScopeMetrics().AppendEmpty()
	m3 := sm2.Metrics().AppendEmpty()
	m3.SetName("metric3")

	// Collect partitions
	var keys [][]byte
	for key := range messenger.partitionData(md) {
		keys = append(keys, key)
	}

	// Should get 2 partitions (one per resource)
	require.Len(t, keys, 2)

	// Verify keys are different
	require.NotEqual(t, string(keys[0]), string(keys[1]))
}

// TestPartitionByResourceAndMetric tests the new partitioning strategy (by resource + metric)
func TestPartitionByResourceAndMetric(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource_and_metric"

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	// Create metrics with single resource but multiple metrics
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "service-a")
	sm := rm.ScopeMetrics().AppendEmpty()

	// Add 3 metrics
	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("metric1")
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("metric2")
	m3 := sm.Metrics().AppendEmpty()
	m3.SetName("metric3")

	// Collect partitions
	var keys [][]byte
	var metrics []pmetric.Metrics
	for key, metricData := range messenger.partitionData(md) {
		keys = append(keys, key)
		metrics = append(metrics, metricData)
	}

	// Should get 3 partitions (one per metric)
	require.Len(t, keys, 3)

	// Verify all keys are different
	keySet := make(map[string]bool)
	for _, key := range keys {
		keyStr := string(key)
		require.False(t, keySet[keyStr], "duplicate key found")
		keySet[keyStr] = true
	}
	require.Len(t, keySet, 3)

	// Verify each partition contains only one metric
	for _, metricData := range metrics {
		require.Equal(t, 1, metricData.ResourceMetrics().Len())
		require.Equal(t, 1, metricData.ResourceMetrics().At(0).ScopeMetrics().Len())
		require.Equal(t, 1, metricData.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len())
	}
}

// TestPartitionByResourceAndMetric_HotSource tests the hot source scenario
func TestPartitionByResourceAndMetric_HotSource(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource_and_metric"

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	// Simulate hot source with 100 metrics
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "hot-source")
	sm := rm.ScopeMetrics().AppendEmpty()

	for i := 0; i < 100; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("metric%d", i))
	}

	// Collect partitions
	var keys [][]byte
	for key := range messenger.partitionData(md) {
		keys = append(keys, key)
	}

	// Should get 100 partitions (one per metric)
	require.Len(t, keys, 100)

	// Verify all keys are unique
	keySet := make(map[string]bool)
	for _, key := range keys {
		keyStr := string(key)
		require.False(t, keySet[keyStr], "duplicate key found")
		keySet[keyStr] = true
	}
	require.Len(t, keySet, 100)
}

// TestPartitionByResourceAndMetric_MultipleResourcesAndMetrics tests complex scenario
func TestPartitionByResourceAndMetric_MultipleResourcesAndMetrics(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource_and_metric"

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	// Create metrics with 3 resources, each with 2 metrics
	md := pmetric.NewMetrics()

	for i := 0; i < 3; i++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", fmt.Sprintf("service-%d", i))
		sm := rm.ScopeMetrics().AppendEmpty()

		for j := 0; j < 2; j++ {
			m := sm.Metrics().AppendEmpty()
			m.SetName(fmt.Sprintf("metric%d", j))
		}
	}

	// Collect partitions
	var keys [][]byte
	for key := range messenger.partitionData(md) {
		keys = append(keys, key)
	}

	// Should get 6 partitions (3 resources × 2 metrics)
	require.Len(t, keys, 6)

	// Verify all keys are unique
	keySet := make(map[string]bool)
	for _, key := range keys {
		keyStr := string(key)
		require.False(t, keySet[keyStr], "duplicate key found")
		keySet[keyStr] = true
	}
	require.Len(t, keySet, 6)
}

// TestBackwardCompatibility_DefaultBehavior tests that default behavior matches current implementation
func TestBackwardCompatibility_DefaultBehavior(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	// Don't set PartitionMetricsRoutingKey (should default to "resource")

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "service-a")
	sm := rm.ScopeMetrics().AppendEmpty()
	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("metric1")
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("metric2")

	// Collect partitions
	var keys [][]byte
	for key := range messenger.partitionData(md) {
		keys = append(keys, key)
	}

	// Should get 1 partition (partitioned by resource only)
	require.Len(t, keys, 1)
}

// TestPartitionByResourceAndMetric_SameMetricNameDifferentResources tests that same metric name
// from different resources gets different keys
func TestPartitionByResourceAndMetric_SameMetricNameDifferentResources(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource_and_metric"

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	// Create metrics with 2 resources, both with same metric name
	md := pmetric.NewMetrics()

	// Resource 1
	rm1 := md.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("service.name", "service-a")
	sm1 := rm1.ScopeMetrics().AppendEmpty()
	m1 := sm1.Metrics().AppendEmpty()
	m1.SetName("cpu.usage")

	// Resource 2
	rm2 := md.ResourceMetrics().AppendEmpty()
	rm2.Resource().Attributes().PutStr("service.name", "service-b")
	sm2 := rm2.ScopeMetrics().AppendEmpty()
	m2 := sm2.Metrics().AppendEmpty()
	m2.SetName("cpu.usage")

	// Collect partitions
	var keys [][]byte
	for key := range messenger.partitionData(md) {
		keys = append(keys, key)
	}

	// Should get 2 partitions (different resources, even with same metric name)
	require.Len(t, keys, 2)

	// Verify keys are different
	require.NotEqual(t, string(keys[0]), string(keys[1]))
}

// TestPartitionData_NoPartitioning tests that when partitioning is disabled, all metrics go to default partition
func TestPartitionData_NoPartitioning(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = false

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "service-a")
	sm := rm.ScopeMetrics().AppendEmpty()
	m1 := sm.Metrics().AppendEmpty()
	m1.SetName("metric1")
	m2 := sm.Metrics().AppendEmpty()
	m2.SetName("metric2")

	// Collect partitions
	var keys [][]byte
	for key := range messenger.partitionData(md) {
		keys = append(keys, key)
	}

	// Should get 1 partition with nil key
	require.Len(t, keys, 1)
	require.Nil(t, keys[0])
}

// Helper function to create test metric data with multiple sources and metrics
func createTestMetricsWithSourcesAndMetrics(sources, metricNames []string) pmetric.Metrics {
	md := pmetric.NewMetrics()

	for _, source := range sources {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("source", source)
		rm.Resource().Attributes().PutStr("service.name", source)
		rm.Resource().Attributes().PutStr("deployment", "cf-production")
		rm.Resource().Attributes().PutStr("instance_group", "database")

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("otel-collector")

		for idx, metricName := range metricNames {
			m := sm.Metrics().AppendEmpty()
			m.SetName(metricName)

			// Add gauge data point with attributes
			gauge := m.SetEmptyGauge()
			dp := gauge.DataPoints().AppendEmpty()
			dp.SetDoubleValue(42.0 + float64(idx))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			// Add data point attributes
			dp.Attributes().PutStr("host", fmt.Sprintf("host-%s", source))
			dp.Attributes().PutStr("environment", "production")
			dp.Attributes().PutStr("region", "us-west-2")
			dp.Attributes().PutInt("port", 8080+int64(idx))
			dp.Attributes().PutBool("is_active", true)
		}
	}

	return md
}

// Helper function to get standard test sources
func getTestSources() []string {
	return []string{
		"PCF_TAS_cell-1", "PCF_TAS_cell-2", "PCF_TAS_cell-3",
		"PCF_TAS_container-1", "PCF_TAS_container-2",
		"PCF_system_metrics_agent-1", "PCF_system_metrics_agent-2",
		"wavefront-proxy-1", "wavefront-proxy-2", "wavefront-proxy-3",
	}
}

// Helper function to get standard test metric names
func getTestMetricNames() []string {
	return []string{
		"tas.system_metrics_agent.system_cpu_user",
		"tas.system_metrics_agent.system_mem_percent",
		"tas.system_metrics_agent.system_disk_system_percent",
		"tas.rep.cpu",
		"tas.rep.memory",
		"tas.rep.disk",
		"tas.container.request_count",
		"tas.container.spike_seconds_count",
		"tas.bosh_unresponsive_agents",
		"tas.capi.info",
	}
}

// Helper function to validate data point attributes
func validateDataPointAttributes(t *testing.T, dp pmetric.NumberDataPoint, expectedSource string) {
	host, exists := dp.Attributes().Get("host")
	require.True(t, exists, "Data point should have 'host' attribute")
	require.Equal(t, fmt.Sprintf("host-%s", expectedSource), host.AsString())

	env, exists := dp.Attributes().Get("environment")
	require.True(t, exists, "Data point should have 'environment' attribute")
	require.Equal(t, "production", env.AsString())

	region, exists := dp.Attributes().Get("region")
	require.True(t, exists, "Data point should have 'region' attribute")
	require.Equal(t, "us-west-2", region.AsString())

	port, exists := dp.Attributes().Get("port")
	require.True(t, exists, "Data point should have 'port' attribute")
	require.True(t, port.Int() >= 8080 && port.Int() < 8090, "Port should be in expected range")

	isActive, exists := dp.Attributes().Get("is_active")
	require.True(t, exists, "Data point should have 'is_active' attribute")
	require.True(t, isActive.Bool(), "is_active should be true")
}

// Helper function to verify hash uniqueness
func verifyHashUniqueness(t *testing.T, hashes [][]byte, expectedCount int, description string) {
	require.Len(t, hashes, expectedCount, description)

	hashSet := make(map[string]bool)
	for _, hash := range hashes {
		hashStr := string(hash)
		require.False(t, hashSet[hashStr], "Hash collision detected")
		hashSet[hashStr] = true
	}
	require.Len(t, hashSet, expectedCount, "All hashes should be unique")
}

// Helper function to collect partition information for composite key partitioning
type partitionInfo struct {
	hash        []byte
	metrics     pmetric.Metrics
	source      string
	metricName  string
	metricCount int
}

func collectCompositeKeyPartitions(t *testing.T, messenger *kafkaMetricsMessenger, md pmetric.Metrics) []partitionInfo {
	var partitions []partitionInfo
	for hash, metricData := range messenger.partitionData(md) {
		// Extract source and metric name from the partitioned data
		require.Equal(t, 1, metricData.ResourceMetrics().Len(), "Each partition should have exactly 1 resource")
		require.Equal(t, 1, metricData.ResourceMetrics().At(0).ScopeMetrics().Len(), "Each partition should have exactly 1 scope")
		require.Equal(t, 1, metricData.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len(), "Each partition should have exactly 1 metric")

		source, _ := metricData.ResourceMetrics().At(0).Resource().Attributes().Get("source")
		metricName := metricData.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Name()

		partitions = append(partitions, partitionInfo{
			hash:        hash,
			metrics:     metricData,
			source:      source.AsString(),
			metricName:  metricName,
			metricCount: 1,
		})
	}
	return partitions
}

// Helper function to collect partition information for resource-only partitioning
func collectResourceOnlyPartitions(t *testing.T, messenger *kafkaMetricsMessenger, md pmetric.Metrics) []partitionInfo {
	var partitions []partitionInfo
	for hash, metricData := range messenger.partitionData(md) {
		require.Equal(t, 1, metricData.ResourceMetrics().Len(), "Each partition should have exactly 1 resource")

		source, _ := metricData.ResourceMetrics().At(0).Resource().Attributes().Get("source")
		metricCount := 0
		for i := 0; i < metricData.ResourceMetrics().At(0).ScopeMetrics().Len(); i++ {
			metricCount += metricData.ResourceMetrics().At(0).ScopeMetrics().At(i).Metrics().Len()
		}

		partitions = append(partitions, partitionInfo{
			hash:        hash,
			metrics:     metricData,
			source:      source.AsString(),
			metricCount: metricCount,
		})
	}
	return partitions
}

// Helper function to verify partition data integrity for composite key partitioning
func verifyCompositeKeyPartitionData(t *testing.T, p partitionInfo) {
	rm := p.metrics.ResourceMetrics().At(0)

	// Verify resource attributes
	source, exists := rm.Resource().Attributes().Get("source")
	require.True(t, exists, "Resource should have 'source' attribute")
	require.Equal(t, p.source, source.AsString())

	serviceName, exists := rm.Resource().Attributes().Get("service.name")
	require.True(t, exists, "Resource should have 'service.name' attribute")
	require.Equal(t, p.source, serviceName.AsString())

	// Verify metric name
	metric := rm.ScopeMetrics().At(0).Metrics().At(0)
	require.Equal(t, p.metricName, metric.Name())

	// Verify metric has data points
	require.Equal(t, 1, metric.Gauge().DataPoints().Len(), "Metric should have 1 data point")

	// Verify data point attributes are preserved
	dp := metric.Gauge().DataPoints().At(0)
	validateDataPointAttributes(t, dp, p.source)
}

// Helper function to verify hash distribution across sources and metrics
func verifyHashDistribution(t *testing.T, partitions []partitionInfo, sources, metricNames []string) {
	// Build source -> metric -> hash mapping
	sourceMetricHashes := make(map[string]map[string]string)
	for _, p := range partitions {
		if _, exists := sourceMetricHashes[p.source]; !exists {
			sourceMetricHashes[p.source] = make(map[string]string)
		}
		sourceMetricHashes[p.source][p.metricName] = string(p.hash)
	}

	// Build metric -> source -> hash mapping
	metricSourceHashes := make(map[string]map[string]string)
	for _, p := range partitions {
		if _, exists := metricSourceHashes[p.metricName]; !exists {
			metricSourceHashes[p.metricName] = make(map[string]string)
		}
		metricSourceHashes[p.metricName][p.source] = string(p.hash)
	}

	// Verify each source has unique hashes for all metrics
	for source, metricHashes := range sourceMetricHashes {
		require.Len(t, metricHashes, len(metricNames), "Source %s should have %d different metric hashes", source, len(metricNames))

		hashValues := make(map[string]bool)
		for _, hash := range metricHashes {
			require.False(t, hashValues[hash], "Duplicate hash for source=%s", source)
			hashValues[hash] = true
		}
	}

	// Verify each metric has unique hashes for all sources
	for metricName, sourceHashes := range metricSourceHashes {
		require.Len(t, sourceHashes, len(sources), "Metric %s should have %d different source hashes", metricName, len(sources))

		hashValues := make(map[string]bool)
		for _, hash := range sourceHashes {
			require.False(t, hashValues[hash], "Duplicate hash for metric=%s", metricName)
			hashValues[hash] = true
		}
	}

	// Verify hash changes when metric name changes (for same source)
	firstSource := sources[0]
	firstSourceHashes := sourceMetricHashes[firstSource]
	hash1 := firstSourceHashes[metricNames[0]]
	hash2 := firstSourceHashes[metricNames[1]]
	require.NotEqual(t, hash1, hash2, "Different metrics from same source should have different hashes")

	// Verify hash changes when source changes (for same metric)
	firstMetric := metricNames[0]
	firstMetricHashes := metricSourceHashes[firstMetric]
	hash3 := firstMetricHashes[sources[0]]
	hash4 := firstMetricHashes[sources[1]]
	require.NotEqual(t, hash3, hash4, "Same metric from different sources should have different hashes")
}

// TestPartitionByResourceAndMetric_ValidateHashWithMetricName tests that the hash
// includes both resource attributes and metric name for composite key partitioning
func TestPartitionByResourceAndMetric_ValidateHashWithMetricName(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource_and_metric"

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	// Get test data
	sources := getTestSources()
	metricNames := getTestMetricNames()
	md := createTestMetricsWithSourcesAndMetrics(sources, metricNames)

	// Collect all partitions with their hashes and data
	partitions := collectCompositeKeyPartitions(t, messenger, md)

	// Verify total partitions: 10 sources × 10 metrics = 100 partitions
	require.Len(t, partitions, 100, "Should have 100 partitions (10 sources × 10 metrics)")

	// Verify all hashes are unique
	var hashes [][]byte
	for _, p := range partitions {
		hashes = append(hashes, p.hash)
	}
	verifyHashUniqueness(t, hashes, 100, "All hashes should be unique")

	// Verify hash distribution across sources and metrics
	verifyHashDistribution(t, partitions, sources, metricNames)

	// Verify each partition contains correct data
	for _, p := range partitions {
		verifyCompositeKeyPartitionData(t, p)
	}
}

// TestPartitionByResource_ValidateHashWithResourceOnly tests that the hash
// includes only resource attributes (not metric name) for resource-only partitioning
func TestPartitionByResource_ValidateHashWithResourceOnly(t *testing.T) {
	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource"

	messenger := &kafkaMetricsMessenger{
		config: *config,
	}

	// Get test data (same as previous test)
	sources := getTestSources()
	metricNames := getTestMetricNames()
	md := createTestMetricsWithSourcesAndMetrics(sources, metricNames)

	// Collect all partitions with their hashes and data
	partitions := collectResourceOnlyPartitions(t, messenger, md)

	// Verify total partitions: 10 sources (NOT 10 sources × 10 metrics)
	require.Len(t, partitions, 10, "Should have 10 partitions (one per source)")

	// Verify all hashes are unique
	var hashes [][]byte
	for _, p := range partitions {
		hashes = append(hashes, p.hash)
	}
	verifyHashUniqueness(t, hashes, 10, "All hashes should be unique")

	// Verify each partition contains ALL metrics for that source
	for _, p := range partitions {
		require.Equal(t, 10, p.metricCount, "Each partition should contain all 10 metrics for source=%s", p.source)

		rm := p.metrics.ResourceMetrics().At(0)

		// Verify resource attributes
		source, exists := rm.Resource().Attributes().Get("source")
		require.True(t, exists, "Resource should have 'source' attribute")
		require.Equal(t, p.source, source.AsString())

		serviceName, exists := rm.Resource().Attributes().Get("service.name")
		require.True(t, exists, "Resource should have 'service.name' attribute")
		require.Equal(t, p.source, serviceName.AsString())

		// Verify all 10 metrics are present
		metricNamesFound := make(map[string]bool)
		for i := 0; i < rm.ScopeMetrics().Len(); i++ {
			for j := 0; j < rm.ScopeMetrics().At(i).Metrics().Len(); j++ {
				metricName := rm.ScopeMetrics().At(i).Metrics().At(j).Name()
				metricNamesFound[metricName] = true
			}
		}
		require.Len(t, metricNamesFound, 10, "Should have all 10 unique metric names for source=%s", p.source)

		// Verify all expected metric names are present
		for _, expectedMetric := range metricNames {
			require.True(t, metricNamesFound[expectedMetric], "Metric %s should be present for source=%s", expectedMetric, p.source)
		}
	}

	// Verify that hash is based ONLY on resource attributes (not metric names)
	// This means same source should always get same hash regardless of metrics
	sourceHashes := make(map[string]string)
	for _, p := range partitions {
		sourceHashes[p.source] = string(p.hash)
	}

	// Create another metrics batch with same sources but different metric names
	md2 := pmetric.NewMetrics()
	for _, source := range sources {
		rm := md2.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("source", source)
		rm.Resource().Attributes().PutStr("service.name", source)
		rm.Resource().Attributes().PutStr("deployment", "cf-production")
		rm.Resource().Attributes().PutStr("instance_group", "database")

		sm := rm.ScopeMetrics().AppendEmpty()

		// Different metric names
		m := sm.Metrics().AppendEmpty()
		m.SetName("completely.different.metric")
		gauge := m.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(99.0)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	}

	// Partition the second batch
	var partitions2 []partitionInfo
	for hash, metricData := range messenger.partitionData(md2) {
		source, _ := metricData.ResourceMetrics().At(0).Resource().Attributes().Get("source")
		partitions2 = append(partitions2, partitionInfo{
			hash:   hash,
			source: source.AsString(),
		})
	}

	// Verify hashes are the SAME for same sources (even with different metrics)
	for _, p := range partitions2 {
		expectedHash := sourceHashes[p.source]
		actualHash := string(p.hash)
		require.Equal(t, expectedHash, actualHash,
			"Hash should be same for source=%s regardless of metric names (resource-only partitioning)", p.source)
	}
}

// TestPartitionByResourceAndMetric_CompareWithResourceOnly validates the difference
// between resource-only and resource+metric partitioning strategies
func TestPartitionByResourceAndMetric_CompareWithResourceOnly(t *testing.T) {
	// Create test data with 5 sources and 5 metrics each
	sources := []string{"source-1", "source-2", "source-3", "source-4", "source-5"}
	metricNames := []string{"cpu.usage", "memory.usage", "disk.io", "network.rx", "network.tx"}

	md := pmetric.NewMetrics()
	for _, source := range sources {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("source", source)
		rm.Resource().Attributes().PutStr("service.name", source)

		sm := rm.ScopeMetrics().AppendEmpty()
		for idx, metricName := range metricNames {
			m := sm.Metrics().AppendEmpty()
			m.SetName(metricName)
			gauge := m.SetEmptyGauge()
			dp := gauge.DataPoints().AppendEmpty()
			dp.SetDoubleValue(42.0 + float64(idx))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			// Add data point attributes
			dp.Attributes().PutStr("host", fmt.Sprintf("host-%s", source))
			dp.Attributes().PutStr("datacenter", "dc1")
			dp.Attributes().PutInt("instance_id", int64(idx+1))
		}
	}

	// Test 1: Resource-only partitioning
	configResource := createDefaultConfig().(*Config)
	configResource.PartitionMetricsByResourceAttributes = true
	configResource.PartitionMetricsRoutingKey = "resource"

	messengerResource := &kafkaMetricsMessenger{config: *configResource}

	var resourcePartitions [][]byte
	for hash := range messengerResource.partitionData(md) {
		resourcePartitions = append(resourcePartitions, hash)
	}

	// Test 2: Resource+Metric partitioning
	configComposite := createDefaultConfig().(*Config)
	configComposite.PartitionMetricsByResourceAttributes = true
	configComposite.PartitionMetricsRoutingKey = "resource_and_metric"

	messengerComposite := &kafkaMetricsMessenger{config: *configComposite}

	var compositePartitions [][]byte
	for hash := range messengerComposite.partitionData(md) {
		compositePartitions = append(compositePartitions, hash)
	}

	// Verify partition counts
	require.Len(t, resourcePartitions, 5, "Resource-only should create 5 partitions (one per source)")
	require.Len(t, compositePartitions, 25, "Resource+Metric should create 25 partitions (5 sources × 5 metrics)")

	// Verify that composite partitioning creates more granular distribution
	require.Equal(t, 5, len(resourcePartitions), "Resource-only: 5 partitions")
	require.Equal(t, 25, len(compositePartitions), "Resource+Metric: 25 partitions")
	require.Equal(t, 5.0, float64(len(compositePartitions))/float64(len(resourcePartitions)),
		"Composite partitioning should create 5x more partitions (one per metric)")
}
