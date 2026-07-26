// Package config loads and validates the broker configuration for mini-kafka.
//
// The configuration is a single YAML file (see config/broker.yaml) that
// describes the broker identity, the log segment policy, topic defaults,
// replication parameters, consumer-group coordinator settings and the static
// cluster membership. Load reads and parses the file; WithDefaults fills in any
// zero-valued field with the package default so the rest of the broker can
// rely on every field being populated.
package config

import (
	"fmt"
	"math"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration object mirroring the structure of
// broker.yaml. Every section is a nested struct tagged with the corresponding
// YAML key.
type Config struct {
	Broker      BrokerConfig      `yaml:"broker"`
	Log         LogConfig         `yaml:"log"`
	Topic       TopicConfig       `yaml:"topic"`
	Replication ReplicationConfig `yaml:"replication"`
	Group       GroupConfig       `yaml:"group"`
	Cluster     ClusterConfig     `yaml:"cluster"`
}

// BrokerConfig holds the identity and network parameters of a single broker.
type BrokerConfig struct {
	// ID is the unique numeric identifier of the broker within the cluster.
	ID int `yaml:"id"`
	// Host is the bind address the broker listens on.
	Host string `yaml:"host"`
	// Port is the TCP port the broker listens on.
	Port int `yaml:"port"`
	// DataDir is the filesystem directory where log segments and indices are
	// stored.
	DataDir string `yaml:"data_dir"`
	// MaxConnections is the maximum number of concurrent client connections
	// the broker will accept.
	MaxConnections int `yaml:"max_connections"`
	// RequestTimeoutMs is the default server-side timeout, in milliseconds,
	// applied to requests that do not carry their own timeout.
	RequestTimeoutMs int `yaml:"request_timeout_ms"`
	// LogLevel controls the verbosity of the structured logger (slog) used by
	// the broker. Accepted values are "debug", "info", "warn" and "error";
	// any other value falls back to "info".
	LogLevel string `yaml:"log_level"`
}

// LogConfig holds the log segment and retention policy shared by all
// partitions.
type LogConfig struct {
	// SegmentBytes is the maximum size, in bytes, of a single log segment
	// before it is rolled.
	SegmentBytes int64 `yaml:"segment_bytes"`
	// SegmentMs is the maximum age, in milliseconds, of a log segment before
	// it is rolled.
	SegmentMs int64 `yaml:"segment_ms"`
	// IndexIntervalBytes is the number of bytes written to the .log file
	// between two consecutive index entries.
	IndexIntervalBytes int64 `yaml:"index_interval_bytes"`
	// IndexMaxBytes is the preallocated size of a segment's index file.
	IndexMaxBytes int64 `yaml:"index_max_bytes"`
	// RetentionMs is the maximum age, in milliseconds, a log segment may
	// reach before it is eligible for deletion. A value of -1 disables
	// time-based retention.
	RetentionMs int64 `yaml:"retention_ms"`
	// RetentionBytes is the maximum total size, in bytes, of a partition's
	// log before the oldest segments are eligible for deletion. A value of
	// -1 disables size-based retention.
	RetentionBytes int64 `yaml:"retention_bytes"`
	// FlushMessages is the number of appended records between forced fsync
	// calls. A value of 0 leaves flushing to the operating system.
	FlushMessages int64 `yaml:"flush_messages"`
	// FlushMs is the maximum time, in milliseconds, between forced fsync
	// calls.
	FlushMs int64 `yaml:"flush_ms"`
	// MaxMessageBytes is the maximum size, in bytes, of a single record
	// batch accepted by Produce.
	MaxMessageBytes int64 `yaml:"max_message_bytes"`
}

// TopicConfig holds the defaults applied when a topic is auto-created or
// created without explicit parameters.
type TopicConfig struct {
	// AutoCreate controls whether missing topics are created on first
	// access.
	AutoCreate bool `yaml:"auto_create"`
	// DefaultPartitions is the partition count assigned to auto-created
	// topics.
	DefaultPartitions int `yaml:"default_partitions"`
	// DefaultReplicationFactor is the replication factor assigned to
	// auto-created topics.
	DefaultReplicationFactor int `yaml:"default_replication_factor"`
}

// ReplicationConfig holds the parameters governing follower fetch and ISR
// tracking.
type ReplicationConfig struct {
	// ReplicaLagTimeMaxMs is the maximum time, in milliseconds, a replica
	// may lag behind the leader before it is removed from the ISR.
	ReplicaLagTimeMaxMs int64 `yaml:"replica_lag_time_max_ms"`
	// ReplicaFetchMaxBytes is the maximum bytes a follower fetches in a
	// single request.
	ReplicaFetchMaxBytes int32 `yaml:"replica_fetch_max_bytes"`
	// ReplicaFetchWaitMaxMs is the maximum time, in milliseconds, a leader
	// waits for enough bytes to accumulate before answering a follower
	// fetch.
	ReplicaFetchWaitMaxMs int64 `yaml:"replica_fetch_wait_max_ms"`
	// MinInsyncReplicas is the minimum number of in-sync replicas (including
	// the leader) required for a Produce with acks=-1 to succeed.
	MinInsyncReplicas int `yaml:"min_insync_replicas"`
}

// GroupConfig holds the consumer-group coordinator parameters.
type GroupConfig struct {
	// SessionTimeoutMs is the maximum time, in milliseconds, a member may
	// go without sending a heartbeat before it is considered dead.
	SessionTimeoutMs int64 `yaml:"session_timeout_ms"`
	// HeartbeatIntervalMs is the interval, in milliseconds, between
	// heartbeat requests sent by a group member.
	HeartbeatIntervalMs int64 `yaml:"heartbeat_interval_ms"`
	// RebalanceTimeoutMs is the maximum time, in milliseconds, the
	// coordinator waits for all members to rejoin during a rebalance.
	RebalanceTimeoutMs int64 `yaml:"rebalance_timeout_ms"`
	// OffsetsTopicPartitions is the number of partitions of the internal
	// __consumer_offsets topic.
	OffsetsTopicPartitions int `yaml:"offsets_topic_partitions"`
}

// ClusterBroker describes a single broker entry in the static cluster
// membership.
type ClusterBroker struct {
	// ID is the unique numeric identifier of the broker.
	ID int `yaml:"id"`
	// Host is the advertised hostname of the broker.
	Host string `yaml:"host"`
	// Port is the advertised TCP port of the broker.
	Port int `yaml:"port"`
}

// ClusterConfig holds the static cluster membership used for leader
// assignment and inter-broker fetch.
type ClusterConfig struct {
	// Brokers is the list of brokers that make up the cluster.
	Brokers []ClusterBroker `yaml:"brokers"`
}

// Load reads the YAML configuration file at path and unmarshals it into a
// Config. After parsing, WithDefaults is applied so the returned Config has
// every field populated. It returns an error if the file cannot be read or
// parsed.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}

	cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: validate %q: %w", path, err)
	}
	return &cfg, nil
}

// Validate checks c for values that would cause silent corruption or undefined
// behaviour at runtime. It returns an error describing the first invalid
// field, or nil if the configuration is sound.
//
// Validate is called by Load after WithDefaults has populated any zero-valued
// fields, so it always sees a fully-populated Config. It may also be called
// directly on a Config that was constructed in code (e.g. in tests).
func (c *Config) Validate() error {
	// The on-disk index stores record positions as uint32. A segment larger
	// than 4 GiB would silently wrap the position field and point reads at the
	// wrong byte offset. Reject this at config time so the broker never starts
	// with a segment policy it cannot index correctly.
	if c.Log.SegmentBytes > math.MaxUint32 {
		return fmt.Errorf("segment_bytes 4 GiB'ı (%d) aşamaz: index pozisyonları uint32", int64(math.MaxUint32))
	}
	// RequestTimeoutMs becomes the server's IdleTimeout. A non-positive value
	// would disable the idle read deadline, leaving the broker vulnerable to
	// slowloris-style resource exhaustion. It must also be greater than the
	// maximum long-poll MaxWaitMs the broker honors (enforced at request time
	// in the fetch handler) so long-polling fetches are not aborted by the
	// idle deadline before they complete.
	if c.Broker.RequestTimeoutMs <= 0 {
		return fmt.Errorf("broker.request_timeout_ms pozitif olmalı (şu an %d)", c.Broker.RequestTimeoutMs)
	}
	return nil
}

// Index entry sizing constants used to derive the index file size from the
// segment policy. These mirror internal/storage.entrySize; they are duplicated
// here to keep the config package free of a storage dependency.
const (
	// indexEntrySize is the fixed on-disk size of a single index entry, in
	// bytes (4 bytes relative offset + 4 bytes position).
	indexEntrySize int64 = 8
	// indexMaxBytesMin is the lower clamp for the derived index size.
	indexMaxBytesMin int64 = 64 * 1024 // 64 KiB
	// indexMaxBytesMax is the upper clamp for the derived index size.
	indexMaxBytesMax int64 = 10 * 1024 * 1024 // 10 MiB
)

// deriveIndexMaxBytes computes a sensible preallocated size for a segment's
// index file from the segment policy. The number of index entries a full
// segment can produce is at most SegmentBytes/IndexIntervalBytes + 1; each
// entry occupies indexEntrySize bytes on disk, and we double the result to
// leave headroom for sparse writes and future growth. The value is clamped to
// [indexMaxBytesMin, indexMaxBytesMax] so degenerate inputs do not produce an
// unusably small or wastefully large index.
//
// If interval is non-positive (should not happen after WithDefaults, but
// guarded for safety) the upper clamp is returned as a conservative default.
func deriveIndexMaxBytes(segmentBytes, indexIntervalBytes int64) int64 {
	if indexIntervalBytes <= 0 {
		return indexMaxBytesMax
	}
	needed := (segmentBytes/indexIntervalBytes + 1) * indexEntrySize * 2
	if needed < indexMaxBytesMin {
		needed = indexMaxBytesMin
	}
	if needed > indexMaxBytesMax {
		needed = indexMaxBytesMax
	}
	return needed
}

// WithDefaults populates any zero-valued field of c with the package default.
// It mutates c in place. The defaults mirror the values shipped in
// config/broker.yaml and the recommended values in docs/PROTOCOL.md.
func (c *Config) WithDefaults() {
	// Broker
	if c.Broker.ID == 0 {
		c.Broker.ID = 1
	}
	if c.Broker.Host == "" {
		c.Broker.Host = "0.0.0.0"
	}
	if c.Broker.Port == 0 {
		c.Broker.Port = 9092
	}
	if c.Broker.DataDir == "" {
		c.Broker.DataDir = "/var/lib/mini-kafka"
	}
	if c.Broker.MaxConnections == 0 {
		c.Broker.MaxConnections = 1024
	}
	if c.Broker.RequestTimeoutMs == 0 {
		c.Broker.RequestTimeoutMs = 30000
	}
	if c.Broker.LogLevel == "" {
		c.Broker.LogLevel = "info"
	}

	// Log
	if c.Log.SegmentBytes == 0 {
		c.Log.SegmentBytes = 128 * 1024 * 1024 // 128 MiB
	}
	if c.Log.SegmentMs == 0 {
		c.Log.SegmentMs = 7 * 24 * 60 * 60 * 1000 // 7 days
	}
	if c.Log.IndexIntervalBytes == 0 {
		c.Log.IndexIntervalBytes = 4096
	}
	if c.Log.IndexMaxBytes == 0 {
		c.Log.IndexMaxBytes = deriveIndexMaxBytes(c.Log.SegmentBytes, c.Log.IndexIntervalBytes)
	}
	if c.Log.RetentionMs == 0 {
		c.Log.RetentionMs = 7 * 24 * 60 * 60 * 1000 // 7 days
	}
	// RetentionBytes: 0 is not a meaningful value; -1 means unlimited.
	if c.Log.RetentionBytes == 0 {
		c.Log.RetentionBytes = -1
	}
	// FlushMessages: 0 is the meaningful default (leave to OS); no override.
	if c.Log.FlushMs == 0 {
		c.Log.FlushMs = 1000
	}
	if c.Log.MaxMessageBytes == 0 {
		c.Log.MaxMessageBytes = 1024 * 1024 // 1 MiB
	}

	// Topic
	// AutoCreate: false is the meaningful default; no override needed.
	if c.Topic.DefaultPartitions == 0 {
		c.Topic.DefaultPartitions = 3
	}
	if c.Topic.DefaultReplicationFactor == 0 {
		c.Topic.DefaultReplicationFactor = 1
	}

	// Replication
	if c.Replication.ReplicaLagTimeMaxMs == 0 {
		c.Replication.ReplicaLagTimeMaxMs = 30000
	}
	if c.Replication.ReplicaFetchMaxBytes == 0 {
		c.Replication.ReplicaFetchMaxBytes = 1048576 // 1 MiB
	}
	if c.Replication.ReplicaFetchWaitMaxMs == 0 {
		c.Replication.ReplicaFetchWaitMaxMs = 500
	}
	if c.Replication.MinInsyncReplicas == 0 {
		c.Replication.MinInsyncReplicas = 1
	}

	// Group
	if c.Group.SessionTimeoutMs == 0 {
		c.Group.SessionTimeoutMs = 45000
	}
	if c.Group.HeartbeatIntervalMs == 0 {
		c.Group.HeartbeatIntervalMs = 3000
	}
	if c.Group.RebalanceTimeoutMs == 0 {
		c.Group.RebalanceTimeoutMs = 60000
	}
	if c.Group.OffsetsTopicPartitions == 0 {
		c.Group.OffsetsTopicPartitions = 50
	}
}
