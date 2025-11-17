package kafka // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka"

import (
	"context"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
)

type AdaptivePartitioner interface {
	kgo.Partitioner
	Connect(*kgo.Client) error
}

type adaptivePartitioner struct {
	primary           kgo.Partitioner

	adminClient       *kadm.Client

	topicPartitioners map[string]*adaptiveTopicPartitioner
	lock              sync.Mutex

	// config
	redirectRate      float32
	minLagThreshold   int
	lagMultiplier     float32
	monitorInterval   time.Duration
	topics            []string
	consumerGroups    []string
}

type adaptiveTopicPartitioner struct {
	topic              string

	parent             *adaptivePartitioner

	primaryPartitioner kgo.TopicPartitioner

	// map lagged partitions to new partitions
	redirects          map[int]int

	lock               sync.Mutex

	cancelFunc         context.CancelFunc
}

func NewAdaptivePartitioner(primary kgo.Partitioner, config configkafka.AdaptivePartitioningConfig) kgo.Partitioner {
	return &adaptivePartitioner{
		primary:         primary,
		redirectRate:    config.RedirectRate,
		minLagThreshold: config.MinLagThreshold,
		lagMultiplier:   config.LagMultiplier,
		monitorInterval: config.MonitorInterval,
		topics:          config.Topics,
		consumerGroups:  config.ConsumerGroups,
	}
}

func (ap *adaptivePartitioner) Connect(client *kgo.Client) {
	// Create an admin connection
	ap.adminClient = kadm.NewClient(client)
}

func (ap *adaptivePartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	ap.lock.Lock()
	defer ap.lock.Unlock()

	if topicPartitioner, ok := ap.topicPartitioners[topic]; ok {
		return topicPartitioner
	}

	tp := &adaptiveTopicPartitioner{
		topic:              topic,
		parent:             ap,
		primaryPartitioner: ap.primary.ForTopic(topic),
	}

	if slices.Contains(ap.topics, topic) {
		// start the monitor
		tp.start(context.Background())
	}

	ap.topicPartitioners[topic] = tp

	return tp
}

func (atp *adaptiveTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	return atp.primaryPartitioner.RequiresConsistency(r)
}

func (atp *adaptiveTopicPartitioner) Partition(r *kgo.Record, n int) int {
	atp.lock.Lock()
	defer atp.lock.Unlock()

	partition := atp.primaryPartitioner.Partition(r, n)
	if redirect, ok := atp.redirects[partition]; ok {
		if rand.Float32() >= atp.parent.redirectRate {
			partition = redirect
		}
	}

	return partition
}

func (atp *adaptiveTopicPartitioner) partitionsAreLagged(laggedPartitions []int, n int) {
	atp.lock.Lock()
	defer atp.lock.Unlock()

	newRedirects := make(map[int]int)

	// remove redirects for partitions that are no longer lagged
	for partition, redirect := range atp.redirects {
		if slices.Contains(laggedPartitions, partition) {
			newRedirects[partition] = redirect
		}
	}

	// add new redirects
	for lagged := range laggedPartitions {
		if _, ok := atp.redirects[lagged]; !ok {
			// check 10 slots for a non-lagged partition
			redirect := (lagged + 1) % n
			for i := 0; i < 10; i++ {
				if slices.Contains(laggedPartitions, redirect) {
					redirect = (redirect + 1) % n
				} else {
					break
				}
			}
			newRedirects[lagged] = redirect
		}
	}

	// set new redirects
	atp.redirects = newRedirects
}

func (atp *adaptiveTopicPartitioner) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	atp.cancelFunc = cancel

	ticker := time.NewTicker(atp.parent.monitorInterval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				overloaded, count, err := atp.calculateOverloadedPartitions(ctx)
				if err != nil {
					continue
				}

				atp.partitionsAreLagged(overloaded, count)
			}
		}
	}()
}

func (atp *adaptiveTopicPartitioner) calculateOverloadedPartitions(ctx context.Context) ([]int, int, error) {
	var maxPartition = 0
	overloaded := make([]int, 0)

	// Fetch lag for all consumer groups
	lags, err := atp.parent.adminClient.Lag(ctx, atp.parent.consumerGroups...)
	if err != nil {
		return nil, 1, err
	}

	// Collect all partition lags
	var allLags []int64
	partitionLags := make(map[int32]int64)

	for _, groupLag := range lags {
		// Skip groups with errors
		if groupLag.DescribeErr != nil || groupLag.FetchErr != nil {
			continue
		}

		// Collect lag for the monitored topic
		for topic, partitions := range groupLag.Lag {
			if topic != atp.topic {
				continue
			}

			for partition, lag := range partitions {
				if int(partition) > maxPartition {
					maxPartition = int(partition)
				}
				if lag.Lag > 0 {
					allLags = append(allLags, lag.Lag)
					// Track maximum lag across all consumer groups for this partition
					if existing, ok := partitionLags[partition]; !ok || lag.Lag > existing {
						partitionLags[partition] = lag.Lag
					}
				}
			}
		}
	}

	if len(allLags) == 0 {
		// No lag data available (all consumers caught up or no consumers)
		return overloaded, 0, nil
	}

	// Calculate median lag
	sort.Slice(allLags, func(i, j int) bool { return allLags[i] < allLags[j] })
	median := allLags[len(allLags)/2]
	if len(allLags)%2 == 0 && len(allLags) > 1 {
		median = (median + allLags[len(allLags)/2-1]) / 2
	}

	// Mark partitions as overloaded if they exceed both thresholds
	for partition, lag := range partitionLags {
		// Condition 1: Absolute threshold
		if lag < int64(atp.parent.minLagThreshold) {
			continue
		}

		// Condition 2: Relative threshold (lag must be significantly higher than median)
		// Example: if LagMultiplier = 0.25, then lag must be 4x the median
		if float32(lag) * atp.parent.lagMultiplier < float32(median) {
			continue
		}

		// Both conditions met: mark as overloaded
		overloaded = append(overloaded, int(partition))
	}

	return overloaded, maxPartition + 1, nil
}
