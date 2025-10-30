// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaexporter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/testdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

func configureBenchmark(tb testing.TB) {
	if os.Getenv("BENCHMARK_KAFKA") == "" {
		tb.Skip("Skipping Kafka benchmarks, set BENCHMARK_KAFKA to any value run them, and optionally USE_FRANZ_GO to use franz-go client")
	}
	tb.Helper()
	if ct := os.Getenv("USE_FRANZ_GO"); ct != "" {
		require.NoError(tb,
			featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, true),
		)
		tb.Cleanup(func() {
			require.NoError(tb,
				featuregate.GlobalRegistry().Set(franzGoClientFeatureGateName, false),
			)
		})
	}
}

// NOTE(marclop) These benchmarks target the default localhost:9092 Kafka broker.
// You can run them against a real Kafka cluster or a docker container:
// docker run --rm -d -p 9092:9092 --name broker apache/kafka:4.0.0
// docker exec --workdir /opt/kafka/bin/ -it broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic otlp_logs
// docker exec --workdir /opt/kafka/bin/ -it broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic otlp_metrics
// docker exec --workdir /opt/kafka/bin/ -it broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic otlp_spans

func BenchmarkLogs(b *testing.B) {
	configureBenchmark(b)
	runBenchmarkLogs(b)
}

func runBenchmarkLogs(b *testing.B) {
	config := createDefaultConfig().(*Config)
	config.ProtocolVersion = "2.3.0"

	exp := newLogsExporter(*config, exportertest.NewNopSettings(metadata.Type))
	b.Cleanup(func() { exp.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := exp.Start(ctx, componenttest.NewNopHost())
	require.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		data := testdata.GenerateLogs(10)
		for p.Next() {
			err := exp.exportData(ctx, data)
			require.NoError(b, err)
		}
	})
}

func BenchmarkMetrics(b *testing.B) {
	configureBenchmark(b)
	runBenchmarkMetrics(b)
}

func runBenchmarkMetrics(b *testing.B) {
	config := createDefaultConfig().(*Config)
	config.ProtocolVersion = "2.3.0"

	exp := newMetricsExporter(*config, exportertest.NewNopSettings(metadata.Type))
	b.Cleanup(func() { exp.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := exp.Start(ctx, componenttest.NewNopHost())
	require.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		data := testdata.GenerateMetrics(10)
		for p.Next() {
			err := exp.exportData(ctx, data)
			require.NoError(b, err)
		}
	})
}

func BenchmarkTraces(b *testing.B) {
	configureBenchmark(b)
	runBenchmarkTraces(b)
}

func runBenchmarkTraces(b *testing.B) {
	config := createDefaultConfig().(*Config)
	config.ProtocolVersion = "2.3.0"

	exp := newTracesExporter(*config, exportertest.NewNopSettings(metadata.Type))
	b.Cleanup(func() { exp.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := exp.Start(ctx, componenttest.NewNopHost())
	require.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		data := testdata.GenerateTraces(10)
		for p.Next() {
			err := exp.exportData(ctx, data)
			require.NoError(b, err)
		}
	})
}

func BenchmarkProfiles(b *testing.B) {
	configureBenchmark(b)
	runBenchmarkProfiles(b)
}

func runBenchmarkProfiles(b *testing.B) {
	config := createDefaultConfig().(*Config)
	config.ProtocolVersion = "2.3.0"

	exp := newProfilesExporter(*config, exportertest.NewNopSettings(metadata.Type))
	b.Cleanup(func() { exp.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := exp.Start(ctx, componenttest.NewNopHost())
	require.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		data := testdata.GenerateProfiles(10)
		for p.Next() {
			err := exp.exportData(ctx, data)
			require.NoError(b, err)
		}
	})
}

// BenchmarkPartitionByResource benchmarks the current partitioning strategy
func BenchmarkPartitionByResource(b *testing.B) {
	md := generateMetricsWithMultipleResources(100, 10) // 100 resources, 10 metrics each

	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource"

	messenger := newKafkaMetricsMessenger(*config, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		for range messenger.partitionData(md) {
			count++
		}
		if count != 100 {
			b.Fatalf("expected 100 partitions, got %d", count)
		}
	}
}

// BenchmarkPartitionByResourceAndMetric benchmarks the new partitioning strategy
func BenchmarkPartitionByResourceAndMetric(b *testing.B) {
	md := generateMetricsWithMultipleResources(100, 10) // 100 resources, 10 metrics each

	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource_and_metric"

	messenger := newKafkaMetricsMessenger(*config, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		for range messenger.partitionData(md) {
			count++
		}
		if count != 1000 { // 100 resources × 10 metrics
			b.Fatalf("expected 1000 partitions, got %d", count)
		}
	}
}

// BenchmarkPartitionByResourceAndMetric_HotSource benchmarks with hot source scenario
func BenchmarkPartitionByResourceAndMetric_HotSource(b *testing.B) {
	// Simulate hot source: 1 resource with 1000 metrics
	md := generateMetricsWithMultipleResources(1, 1000)

	config := createDefaultConfig().(*Config)
	config.PartitionMetricsByResourceAttributes = true
	config.PartitionMetricsRoutingKey = "resource_and_metric"

	messenger := newKafkaMetricsMessenger(*config, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		for range messenger.partitionData(md) {
			count++
		}
		if count != 1000 {
			b.Fatalf("expected 1000 partitions, got %d", count)
		}
	}
}

// BenchmarkHashComputation benchmarks hash computation performance
func BenchmarkHashComputation(b *testing.B) {
	resource := pmetric.NewMetrics().ResourceMetrics().AppendEmpty().Resource()
	resource.Attributes().PutStr("service.name", "my-service")
	resource.Attributes().PutStr("host.name", "my-host")
	resource.Attributes().PutStr("deployment.environment", "production")

	metricName := "http.server.request.duration"

	b.Run("ResourceOnly", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pdatautil.MapHash(resource.Attributes())
		}
	})

	b.Run("ResourceAndMetric", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pdatautil.Hash(
				pdatautil.WithMap(resource.Attributes()),
				pdatautil.WithString(metricName),
			)
		}
	})
}

// generateMetricsWithMultipleResources generates test metrics with multiple resources and metrics
func generateMetricsWithMultipleResources(numResources, metricsPerResource int) pmetric.Metrics {
	md := pmetric.NewMetrics()

	for i := 0; i < numResources; i++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", fmt.Sprintf("service-%d", i))
		rm.Resource().Attributes().PutStr("host.name", fmt.Sprintf("host-%d", i%10))

		sm := rm.ScopeMetrics().AppendEmpty()

		for j := 0; j < metricsPerResource; j++ {
			m := sm.Metrics().AppendEmpty()
			m.SetName(fmt.Sprintf("metric%d", j))

			// Add gauge data point
			gauge := m.SetEmptyGauge()
			dp := gauge.DataPoints().AppendEmpty()
			dp.SetIntValue(int64(i * j))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		}
	}

	return md
}

// generateRealisticMetrics generates realistic metrics with source, service.name, and data point attributes
func generateRealisticMetrics(numSources, metricsPerSource int) pmetric.Metrics {
	md := pmetric.NewMetrics()

	// Realistic metric names from production systems
	metricNames := []string{
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
		"http.server.request.duration",
		"http.server.response.size",
		"http.client.request.duration",
		"db.query.duration",
		"cache.hit.ratio",
		"queue.message.count",
		"jvm.memory.used",
		"jvm.gc.duration",
		"process.cpu.utilization",
		"process.memory.usage",
	}

	// Source prefixes for realistic naming
	sourcePrefixes := []string{
		"PCF_TAS_cell",
		"PCF_TAS_container",
		"PCF_system_metrics_agent",
		"wavefront-proxy",
		"k8s-node",
		"docker-host",
		"vm-instance",
		"cloud-function",
	}

	for i := 0; i < numSources; i++ {
		rm := md.ResourceMetrics().AppendEmpty()

		// Generate realistic source name
		sourcePrefix := sourcePrefixes[i%len(sourcePrefixes)]
		sourceName := fmt.Sprintf("%s-%d", sourcePrefix, i)

		// Add only 3 resource attributes: source, service.name, dx_tenant_id
		rm.Resource().Attributes().PutStr("source", sourceName)
		rm.Resource().Attributes().PutStr("service.name", sourceName)
		rm.Resource().Attributes().PutStr("dx_tenant_id", fmt.Sprintf("tenant-%04d", i%100))

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("otel-collector")
		sm.Scope().SetVersion("1.0.0")

		for j := 0; j < metricsPerSource; j++ {
			m := sm.Metrics().AppendEmpty()

			// Use realistic metric names
			metricName := metricNames[j%len(metricNames)]
			if j >= len(metricNames) {
				metricName = fmt.Sprintf("%s.custom_%d", metricName, j/len(metricNames))
			}
			m.SetName(metricName)
			m.SetDescription(fmt.Sprintf("Description for %s", metricName))
			m.SetUnit("1")

			// Add gauge data point with 25 attributes
			gauge := m.SetEmptyGauge()
			dp := gauge.DataPoints().AppendEmpty()
			dp.SetDoubleValue(42.0 + float64(i*j))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

			// Add 25 data point attributes (realistic production scenario)
			dp.Attributes().PutStr("host", fmt.Sprintf("host-%s", sourceName))
			dp.Attributes().PutStr("environment", "production")
			dp.Attributes().PutStr("region", fmt.Sprintf("us-west-%d", (i%3)+1))
			dp.Attributes().PutStr("availability_zone", fmt.Sprintf("az-%d", (i%3)+1))
			dp.Attributes().PutInt("port", 8080+int64(j%10))
			dp.Attributes().PutBool("is_active", true)
			dp.Attributes().PutStr("version", fmt.Sprintf("v1.%d.%d", i%10, j%10))
			dp.Attributes().PutStr("cluster", fmt.Sprintf("cluster-%d", i%5))
			dp.Attributes().PutStr("namespace", fmt.Sprintf("ns-%d", i%10))
			dp.Attributes().PutStr("pod_name", fmt.Sprintf("pod-%s-%d", sourceName, j))
			dp.Attributes().PutStr("container_name", fmt.Sprintf("container-%d", j%5))
			dp.Attributes().PutStr("deployment_name", fmt.Sprintf("deploy-%d", i%20))
			dp.Attributes().PutStr("service_instance_id", fmt.Sprintf("instance-%d-%d", i, j))
			dp.Attributes().PutStr("endpoint", fmt.Sprintf("/api/v1/endpoint-%d", j%10))
			dp.Attributes().PutStr("http_method", []string{"GET", "POST", "PUT", "DELETE"}[j%4])
			dp.Attributes().PutInt("http_status_code", int64(200+((i+j)%6)*100))
			dp.Attributes().PutStr("protocol", "HTTP/1.1")
			dp.Attributes().PutStr("user_agent", fmt.Sprintf("client-%d", i%5))
			dp.Attributes().PutStr("client_ip", fmt.Sprintf("10.%d.%d.%d", i%256, j%256, (i+j)%256))
			dp.Attributes().PutStr("server_ip", fmt.Sprintf("172.16.%d.%d", i%256, j%256))
			dp.Attributes().PutStr("load_balancer", fmt.Sprintf("lb-%d", i%10))
			dp.Attributes().PutStr("instance_type", []string{"t3.medium", "t3.large", "t3.xlarge"}[i%3])
			dp.Attributes().PutStr("os", []string{"linux", "windows"}[i%2])
			dp.Attributes().PutStr("architecture", "x86_64")
			dp.Attributes().PutStr("runtime", fmt.Sprintf("java-%d", 11+(i%6)))
		}
	}

	return md
}

// BenchmarkPartitionByResource_Realistic benchmarks resource-only partitioning with realistic data
func BenchmarkPartitionByResource_Realistic(b *testing.B) {
	benchmarks := []struct {
		name             string
		numSources       int
		metricsPerSource int
	}{
		{"50sources_20metrics", 50, 20},
		{"100sources_50metrics", 100, 50},
		{"200sources_100metrics", 200, 100},
		{"500sources_50metrics", 500, 50},
		{"1000sources_20metrics", 1000, 20},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			md := generateRealisticMetrics(bm.numSources, bm.metricsPerSource)
			expectedPartitions := bm.numSources

			config := createDefaultConfig().(*Config)
			config.PartitionMetricsByResourceAttributes = true
			config.PartitionMetricsRoutingKey = "resource"

			messenger := newKafkaMetricsMessenger(*config, nil)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				count := 0
				for range messenger.partitionData(md) {
					count++
				}
				if count != expectedPartitions {
					b.Fatalf("expected %d partitions, got %d", expectedPartitions, count)
				}
			}
		})
	}
}

// BenchmarkPartitionByResourceAndMetric_Realistic benchmarks composite key partitioning with realistic data
func BenchmarkPartitionByResourceAndMetric_Realistic(b *testing.B) {
	benchmarks := []struct {
		name             string
		numSources       int
		metricsPerSource int
	}{
		{"50sources_20metrics", 50, 20},
		{"100sources_50metrics", 100, 50},
		{"200sources_100metrics", 200, 100},
		{"500sources_50metrics", 500, 50},
		{"1000sources_20metrics", 1000, 20},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			md := generateRealisticMetrics(bm.numSources, bm.metricsPerSource)
			// With batching optimization: one batch per source (not one per metric)
			expectedBatches := bm.numSources

			config := createDefaultConfig().(*Config)
			config.PartitionMetricsByResourceAttributes = true
			config.PartitionMetricsRoutingKey = "resource_and_metric"

			messenger := newKafkaMetricsMessenger(*config, nil)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				count := 0
				for range messenger.partitionData(md) {
					count++
				}
				if count != expectedBatches {
					b.Fatalf("expected %d batches, got %d", expectedBatches, count)
				}
			}
		})
	}
}

// BenchmarkPartitionComparison_Realistic compares both strategies with realistic data
func BenchmarkPartitionComparison_Realistic(b *testing.B) {
	// Simulate production scenario: 100 sources with 50 metrics each
	md := generateRealisticMetrics(100, 50)

	b.Run("ResourceOnly_100x50", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			count := 0
			for range messenger.partitionData(md) {
				count++
			}
			if count != 100 {
				b.Fatalf("expected 100 partitions, got %d", count)
			}
		}
	})

	b.Run("ResourceAndMetric_100x50", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource_and_metric"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			count := 0
			for range messenger.partitionData(md) {
				count++
			}
			// With batching optimization: one batch per source (not one per metric)
			if count != 100 {
				b.Fatalf("expected 100 batches, got %d", count)
			}
		}
	})
}

// BenchmarkHotSourceScenario_Realistic benchmarks hot source scenarios with realistic data
func BenchmarkHotSourceScenario_Realistic(b *testing.B) {
	benchmarks := []struct {
		name             string
		numSources       int
		metricsPerSource int
		description      string
	}{
		{"1hot_source_100metrics", 1, 100, "Single hot source with 100 metrics"},
		{"1hot_source_500metrics", 1, 500, "Single hot source with 500 metrics"},
		{"1hot_source_1000metrics", 1, 1000, "Single hot source with 1000 metrics"},
		{"10hot_sources_100metrics", 10, 100, "10 hot sources with 100 metrics each"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			md := generateRealisticMetrics(bm.numSources, bm.metricsPerSource)

			b.Run("ResourceOnly", func(b *testing.B) {
				config := createDefaultConfig().(*Config)
				config.PartitionMetricsByResourceAttributes = true
				config.PartitionMetricsRoutingKey = "resource"

				messenger := newKafkaMetricsMessenger(*config, nil)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					count := 0
					for range messenger.partitionData(md) {
						count++
					}
					if count != bm.numSources {
						b.Fatalf("expected %d partitions, got %d", bm.numSources, count)
					}
				}
			})

			b.Run("ResourceAndMetric", func(b *testing.B) {
				config := createDefaultConfig().(*Config)
				config.PartitionMetricsByResourceAttributes = true
				config.PartitionMetricsRoutingKey = "resource_and_metric"

				messenger := newKafkaMetricsMessenger(*config, nil)

				b.ResetTimer()
				b.ReportAllocs()

				// With batching optimization: one batch per source (not one per metric)
				expectedBatches := bm.numSources
				for i := 0; i < b.N; i++ {
					count := 0
					for range messenger.partitionData(md) {
						count++
					}
					if count != expectedBatches {
						b.Fatalf("expected %d batches, got %d", expectedBatches, count)
					}
				}
			})
		})
	}
}

// BenchmarkHashComputation_Realistic benchmarks hash computation with realistic attributes
func BenchmarkHashComputation_Realistic(b *testing.B) {
	resource := pmetric.NewMetrics().ResourceMetrics().AppendEmpty().Resource()
	resource.Attributes().PutStr("source", "PCF_TAS_cell-123")
	resource.Attributes().PutStr("service.name", "PCF_TAS_cell-123")
	resource.Attributes().PutStr("dx_tenant_id", "tenant-0042")

	metricName := "tas.system_metrics_agent.system_cpu_user"

	b.Run("ResourceOnly", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pdatautil.MapHash(resource.Attributes())
		}
	})

	b.Run("ResourceAndMetric", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pdatautil.Hash(
				pdatautil.WithMap(resource.Attributes()),
				pdatautil.WithString(metricName),
			)
		}
	})
}

// BenchmarkMemoryComparison_SameInput compares memory usage between strategies with identical input
func BenchmarkMemoryComparison_SameInput(b *testing.B) {
	scenarios := []struct {
		name             string
		numSources       int
		metricsPerSource int
	}{
		{"10sources_10metrics", 10, 10},
		{"50sources_20metrics", 50, 20},
		{"100sources_50metrics", 100, 50},
		{"200sources_100metrics", 200, 100},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			// Generate metrics ONCE - same input for both strategies
			md := generateRealisticMetrics(scenario.numSources, scenario.metricsPerSource)

			// Calculate input data size
			inputResourceMetrics := md.ResourceMetrics().Len()
			inputTotalMetrics := 0
			for i := 0; i < md.ResourceMetrics().Len(); i++ {
				for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
					inputTotalMetrics += md.ResourceMetrics().At(i).ScopeMetrics().At(j).Metrics().Len()
				}
			}

			b.Run("ResourceOnly", func(b *testing.B) {
				config := createDefaultConfig().(*Config)
				config.PartitionMetricsByResourceAttributes = true
				config.PartitionMetricsRoutingKey = "resource"

				messenger := newKafkaMetricsMessenger(*config, nil)

				b.ReportMetric(float64(inputResourceMetrics), "input_resources")
				b.ReportMetric(float64(inputTotalMetrics), "input_metrics")
				b.ResetTimer()
				b.ReportAllocs()

				var totalPartitions int
				var totalResourceMetrics int
				var totalMetrics int

				for i := 0; i < b.N; i++ {
					partitions := 0
					resourceMetrics := 0
					metrics := 0

					for _, partitionedData := range messenger.partitionData(md) {
						partitions++
						resourceMetrics += partitionedData.ResourceMetrics().Len()
						for j := 0; j < partitionedData.ResourceMetrics().Len(); j++ {
							for k := 0; k < partitionedData.ResourceMetrics().At(j).ScopeMetrics().Len(); k++ {
								metrics += partitionedData.ResourceMetrics().At(j).ScopeMetrics().At(k).Metrics().Len()
							}
						}
					}

					totalPartitions = partitions
					totalResourceMetrics = resourceMetrics
					totalMetrics = metrics
				}

				b.ReportMetric(float64(totalPartitions), "output_partitions")
				b.ReportMetric(float64(totalResourceMetrics), "output_resources")
				b.ReportMetric(float64(totalMetrics), "output_metrics")
			})

			b.Run("ResourceAndMetric", func(b *testing.B) {
				config := createDefaultConfig().(*Config)
				config.PartitionMetricsByResourceAttributes = true
				config.PartitionMetricsRoutingKey = "resource_and_metric"

				messenger := newKafkaMetricsMessenger(*config, nil)

				b.ReportMetric(float64(inputResourceMetrics), "input_resources")
				b.ReportMetric(float64(inputTotalMetrics), "input_metrics")
				b.ResetTimer()
				b.ReportAllocs()

				var totalPartitions int
				var totalResourceMetrics int
				var totalMetrics int

				for i := 0; i < b.N; i++ {
					partitions := 0
					resourceMetrics := 0
					metrics := 0

					for _, partitionedData := range messenger.partitionData(md) {
						partitions++
						resourceMetrics += partitionedData.ResourceMetrics().Len()
						for j := 0; j < partitionedData.ResourceMetrics().Len(); j++ {
							for k := 0; k < partitionedData.ResourceMetrics().At(j).ScopeMetrics().Len(); k++ {
								metrics += partitionedData.ResourceMetrics().At(j).ScopeMetrics().At(k).Metrics().Len()
							}
						}
					}

					totalPartitions = partitions
					totalResourceMetrics = resourceMetrics
					totalMetrics = metrics
				}

				b.ReportMetric(float64(totalPartitions), "output_partitions")
				b.ReportMetric(float64(totalResourceMetrics), "output_resources")
				b.ReportMetric(float64(totalMetrics), "output_metrics")
			})
		})
	}
}

// BenchmarkMemoryPerPartition measures memory overhead per partition
func BenchmarkMemoryPerPartition(b *testing.B) {
	scenarios := []struct {
		name             string
		numSources       int
		metricsPerSource int
	}{
		{"1source_100metrics", 1, 100},
		{"1source_500metrics", 1, 500},
		{"1source_1000metrics", 1, 1000},
	}

	for _, scenario := range scenarios {
		md := generateRealisticMetrics(scenario.numSources, scenario.metricsPerSource)

		b.Run(scenario.name, func(b *testing.B) {
			b.Run("ResourceOnly", func(b *testing.B) {
				config := createDefaultConfig().(*Config)
				config.PartitionMetricsByResourceAttributes = true
				config.PartitionMetricsRoutingKey = "resource"

				messenger := newKafkaMetricsMessenger(*config, nil)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					var partitions int
					for range messenger.partitionData(md) {
						partitions++
					}
					if i == 0 {
						b.ReportMetric(float64(partitions), "partitions")
					}
				}
			})

			b.Run("ResourceAndMetric", func(b *testing.B) {
				config := createDefaultConfig().(*Config)
				config.PartitionMetricsByResourceAttributes = true
				config.PartitionMetricsRoutingKey = "resource_and_metric"

				messenger := newKafkaMetricsMessenger(*config, nil)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					var partitions int
					for range messenger.partitionData(md) {
						partitions++
					}
					if i == 0 {
						b.ReportMetric(float64(partitions), "partitions")
					}
				}
			})
		})
	}
}

// BenchmarkMemoryComparison_200K tests with 200,000 metrics (200 sources × 1000 metrics)
func BenchmarkMemoryComparison_200K(b *testing.B) {
	// Generate 200K metrics: 200 sources × 1000 metrics each
	md := generateRealisticMetrics(200, 1000)

	// Calculate input data size
	inputResourceMetrics := md.ResourceMetrics().Len()
	inputTotalMetrics := 0
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
			inputTotalMetrics += md.ResourceMetrics().At(i).ScopeMetrics().At(j).Metrics().Len()
		}
	}

	b.Logf("Input: %d sources, %d total metrics", inputResourceMetrics, inputTotalMetrics)

	b.Run("ResourceOnly", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ReportMetric(float64(inputResourceMetrics), "input_resources")
		b.ReportMetric(float64(inputTotalMetrics), "input_metrics")
		b.ResetTimer()
		b.ReportAllocs()

		var totalPartitions int

		for i := 0; i < b.N; i++ {
			partitions := 0
			for range messenger.partitionData(md) {
				partitions++
			}
			totalPartitions = partitions
		}

		b.ReportMetric(float64(totalPartitions), "output_partitions")
		b.Logf("Resource-Only: %d partitions created", totalPartitions)
	})

	b.Run("ResourceAndMetric", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource_and_metric"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ReportMetric(float64(inputResourceMetrics), "input_resources")
		b.ReportMetric(float64(inputTotalMetrics), "input_metrics")
		b.ResetTimer()
		b.ReportAllocs()

		var totalPartitions int

		for i := 0; i < b.N; i++ {
			partitions := 0
			for range messenger.partitionData(md) {
				partitions++
			}
			totalPartitions = partitions
		}

		b.ReportMetric(float64(totalPartitions), "output_partitions")
		b.Logf("Resource+Metric: %d partitions created", totalPartitions)
	})
}

// BenchmarkMemoryComparison_500K tests with 500,000 metrics (500 sources × 1000 metrics)
func BenchmarkMemoryComparison_500K(b *testing.B) {
	// Generate 500K metrics: 500 sources × 1000 metrics each
	md := generateRealisticMetrics(500, 1000)

	// Calculate input data size
	inputResourceMetrics := md.ResourceMetrics().Len()
	inputTotalMetrics := 0
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
			inputTotalMetrics += md.ResourceMetrics().At(i).ScopeMetrics().At(j).Metrics().Len()
		}
	}

	b.Logf("Input: %d sources, %d total metrics", inputResourceMetrics, inputTotalMetrics)

	b.Run("ResourceOnly", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ReportMetric(float64(inputResourceMetrics), "input_resources")
		b.ReportMetric(float64(inputTotalMetrics), "input_metrics")
		b.ResetTimer()
		b.ReportAllocs()

		var totalPartitions int

		for i := 0; i < b.N; i++ {
			partitions := 0
			for range messenger.partitionData(md) {
				partitions++
			}
			totalPartitions = partitions
		}

		b.ReportMetric(float64(totalPartitions), "output_partitions")
		b.Logf("Resource-Only: %d partitions created", totalPartitions)
	})

	b.Run("ResourceAndMetric", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource_and_metric"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ReportMetric(float64(inputResourceMetrics), "input_resources")
		b.ReportMetric(float64(inputTotalMetrics), "input_metrics")
		b.ResetTimer()
		b.ReportAllocs()

		var totalPartitions int

		for i := 0; i < b.N; i++ {
			partitions := 0
			for range messenger.partitionData(md) {
				partitions++
			}
			totalPartitions = partitions
		}

		b.ReportMetric(float64(totalPartitions), "output_partitions")
		b.Logf("Resource+Metric: %d partitions created", totalPartitions)
	})
}

// BenchmarkBatchingEfficiency compares batched vs non-batched message counts and performance.
// This benchmark demonstrates the impact of batching on message count and throughput.
func BenchmarkBatchingEfficiency(b *testing.B) {
	// Test with realistic scale: 100 sources × 50 metrics = 5,000 metrics
	md := generateRealisticMetrics(100, 50)

	inputResourceMetrics := md.ResourceMetrics().Len()
	inputTotalMetrics := 0
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
			inputTotalMetrics += md.ResourceMetrics().At(i).ScopeMetrics().At(j).Metrics().Len()
		}
	}

	b.Logf("Input: %d sources, %d total metrics", inputResourceMetrics, inputTotalMetrics)

	b.Run("ResourceOnly_Baseline", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ResetTimer()
		b.ReportAllocs()

		var messageCount int
		var totalMetricsInMessages int

		for i := 0; i < b.N; i++ {
			messages := 0
			metricsCount := 0
			for _, batch := range messenger.partitionData(md) {
				messages++
				// Count metrics in this batch
				for ri := 0; ri < batch.ResourceMetrics().Len(); ri++ {
					for si := 0; si < batch.ResourceMetrics().At(ri).ScopeMetrics().Len(); si++ {
						metricsCount += batch.ResourceMetrics().At(ri).ScopeMetrics().At(si).Metrics().Len()
					}
				}
			}
			messageCount = messages
			totalMetricsInMessages = metricsCount
		}

		avgMetricsPerMessage := float64(totalMetricsInMessages) / float64(messageCount)
		b.ReportMetric(float64(messageCount), "messages")
		b.ReportMetric(avgMetricsPerMessage, "metrics/message")
		b.Logf("Messages: %d, Metrics/Message: %.1f", messageCount, avgMetricsPerMessage)
	})

	b.Run("ResourceAndMetric_Batched", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource_and_metric"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ResetTimer()
		b.ReportAllocs()

		var messageCount int
		var totalMetricsInMessages int

		for i := 0; i < b.N; i++ {
			messages := 0
			metricsCount := 0
			for _, batch := range messenger.partitionData(md) {
				messages++
				// Count metrics in this batch
				for ri := 0; ri < batch.ResourceMetrics().Len(); ri++ {
					for si := 0; si < batch.ResourceMetrics().At(ri).ScopeMetrics().Len(); si++ {
						metricsCount += batch.ResourceMetrics().At(ri).ScopeMetrics().At(si).Metrics().Len()
					}
				}
			}
			messageCount = messages
			totalMetricsInMessages = metricsCount
		}

		avgMetricsPerMessage := float64(totalMetricsInMessages) / float64(messageCount)
		b.ReportMetric(float64(messageCount), "messages")
		b.ReportMetric(avgMetricsPerMessage, "metrics/message")
		b.Logf("Messages: %d, Metrics/Message: %.1f", messageCount, avgMetricsPerMessage)
	})
}

// BenchmarkBatchingEfficiency_LargeScale tests batching with larger scale
func BenchmarkBatchingEfficiency_LargeScale(b *testing.B) {
	// Test with larger scale: 500 sources × 100 metrics = 50,000 metrics
	md := generateRealisticMetrics(500, 100)

	inputResourceMetrics := md.ResourceMetrics().Len()
	inputTotalMetrics := 0
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
			inputTotalMetrics += md.ResourceMetrics().At(i).ScopeMetrics().At(j).Metrics().Len()
		}
	}

	b.Logf("Input: %d sources, %d total metrics", inputResourceMetrics, inputTotalMetrics)

	b.Run("ResourceOnly", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ResetTimer()
		b.ReportAllocs()

		var messageCount int
		var totalMetricsInMessages int

		for i := 0; i < b.N; i++ {
			messages := 0
			metricsCount := 0
			for _, batch := range messenger.partitionData(md) {
				messages++
				for ri := 0; ri < batch.ResourceMetrics().Len(); ri++ {
					for si := 0; si < batch.ResourceMetrics().At(ri).ScopeMetrics().Len(); si++ {
						metricsCount += batch.ResourceMetrics().At(ri).ScopeMetrics().At(si).Metrics().Len()
					}
				}
			}
			messageCount = messages
			totalMetricsInMessages = metricsCount
		}

		avgMetricsPerMessage := float64(totalMetricsInMessages) / float64(messageCount)
		b.ReportMetric(float64(messageCount), "messages")
		b.ReportMetric(avgMetricsPerMessage, "metrics/message")
		b.Logf("Messages: %d, Metrics/Message: %.1f", messageCount, avgMetricsPerMessage)
	})

	b.Run("ResourceAndMetric_Batched", func(b *testing.B) {
		config := createDefaultConfig().(*Config)
		config.PartitionMetricsByResourceAttributes = true
		config.PartitionMetricsRoutingKey = "resource_and_metric"

		messenger := newKafkaMetricsMessenger(*config, nil)

		b.ResetTimer()
		b.ReportAllocs()

		var messageCount int
		var totalMetricsInMessages int

		for i := 0; i < b.N; i++ {
			messages := 0
			metricsCount := 0
			for _, batch := range messenger.partitionData(md) {
				messages++
				for ri := 0; ri < batch.ResourceMetrics().Len(); ri++ {
					for si := 0; si < batch.ResourceMetrics().At(ri).ScopeMetrics().Len(); si++ {
						metricsCount += batch.ResourceMetrics().At(ri).ScopeMetrics().At(si).Metrics().Len()
					}
				}
			}
			messageCount = messages
			totalMetricsInMessages = metricsCount
		}

		avgMetricsPerMessage := float64(totalMetricsInMessages) / float64(messageCount)
		b.ReportMetric(float64(messageCount), "messages")
		b.ReportMetric(avgMetricsPerMessage, "metrics/message")
		b.Logf("Messages: %d, Metrics/Message: %.1f", messageCount, avgMetricsPerMessage)
	})
}
