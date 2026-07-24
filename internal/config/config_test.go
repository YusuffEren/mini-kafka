package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeYAML(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}
	return path
}

func validFullYAML() string {
	return `
broker:
  id: 2
  host: "127.0.0.1"
  port: 9093
  data_dir: "/tmp/test-data"
  max_connections: 512
  request_timeout_ms: 10000
log:
  segment_bytes: 67108864
  segment_ms: 86400000
  index_interval_bytes: 8192
  index_max_bytes: 5242880
  retention_ms: 86400000
  retention_bytes: -1
  flush_messages: 100
  flush_ms: 2000
  max_message_bytes: 524288
topic:
  auto_create: true
  default_partitions: 6
  default_replication_factor: 2
replication:
  replica_lag_time_max_ms: 15000
  replica_fetch_max_bytes: 524288
  replica_fetch_wait_max_ms: 250
  min_insync_replicas: 2
group:
  session_timeout_ms: 30000
  heartbeat_interval_ms: 5000
  rebalance_timeout_ms: 45000
  offsets_topic_partitions: 25
cluster:
  brokers:
    - id: 1
      host: "broker1"
      port: 9092
`
}

func TestLoad_gecerli_broker_yaml_basarili_okur(t *testing.T) {
	path := writeYAML(t, t.TempDir(), "broker.yaml", validFullYAML())

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if cfg.Broker.ID != 2 {
		t.Errorf("Broker.ID = %d, want 2", cfg.Broker.ID)
	}
	if cfg.Broker.Host != "127.0.0.1" {
		t.Errorf("Broker.Host = %q, want 127.0.0.1", cfg.Broker.Host)
	}
	if cfg.Broker.Port != 9093 {
		t.Errorf("Broker.Port = %d, want 9093", cfg.Broker.Port)
	}
	if cfg.Broker.DataDir != "/tmp/test-data" {
		t.Errorf("Broker.DataDir = %q, want /tmp/test-data", cfg.Broker.DataDir)
	}
	if cfg.Broker.MaxConnections != 512 {
		t.Errorf("Broker.MaxConnections = %d, want 512", cfg.Broker.MaxConnections)
	}
	if cfg.Broker.RequestTimeoutMs != 10000 {
		t.Errorf("Broker.RequestTimeoutMs = %d, want 10000", cfg.Broker.RequestTimeoutMs)
	}

	if cfg.Log.SegmentBytes != 67108864 {
		t.Errorf("Log.SegmentBytes = %d, want 67108864", cfg.Log.SegmentBytes)
	}
	if cfg.Log.SegmentMs != 86400000 {
		t.Errorf("Log.SegmentMs = %d, want 86400000", cfg.Log.SegmentMs)
	}
	if cfg.Log.IndexIntervalBytes != 8192 {
		t.Errorf("Log.IndexIntervalBytes = %d, want 8192", cfg.Log.IndexIntervalBytes)
	}
	if cfg.Log.IndexMaxBytes != 5242880 {
		t.Errorf("Log.IndexMaxBytes = %d, want 5242880", cfg.Log.IndexMaxBytes)
	}
	if cfg.Log.RetentionMs != 86400000 {
		t.Errorf("Log.RetentionMs = %d, want 86400000", cfg.Log.RetentionMs)
	}
	if cfg.Log.RetentionBytes != -1 {
		t.Errorf("Log.RetentionBytes = %d, want -1", cfg.Log.RetentionBytes)
	}
	if cfg.Log.FlushMessages != 100 {
		t.Errorf("Log.FlushMessages = %d, want 100", cfg.Log.FlushMessages)
	}
	if cfg.Log.FlushMs != 2000 {
		t.Errorf("Log.FlushMs = %d, want 2000", cfg.Log.FlushMs)
	}
	if cfg.Log.MaxMessageBytes != 524288 {
		t.Errorf("Log.MaxMessageBytes = %d, want 524288", cfg.Log.MaxMessageBytes)
	}

	if !cfg.Topic.AutoCreate {
		t.Errorf("Topic.AutoCreate = false, want true")
	}
	if cfg.Topic.DefaultPartitions != 6 {
		t.Errorf("Topic.DefaultPartitions = %d, want 6", cfg.Topic.DefaultPartitions)
	}
	if cfg.Topic.DefaultReplicationFactor != 2 {
		t.Errorf("Topic.DefaultReplicationFactor = %d, want 2", cfg.Topic.DefaultReplicationFactor)
	}

	if cfg.Replication.ReplicaLagTimeMaxMs != 15000 {
		t.Errorf("Replication.ReplicaLagTimeMaxMs = %d, want 15000", cfg.Replication.ReplicaLagTimeMaxMs)
	}
	if cfg.Replication.ReplicaFetchMaxBytes != 524288 {
		t.Errorf("Replication.ReplicaFetchMaxBytes = %d, want 524288", cfg.Replication.ReplicaFetchMaxBytes)
	}
	if cfg.Replication.ReplicaFetchWaitMaxMs != 250 {
		t.Errorf("Replication.ReplicaFetchWaitMaxMs = %d, want 250", cfg.Replication.ReplicaFetchWaitMaxMs)
	}
	if cfg.Replication.MinInsyncReplicas != 2 {
		t.Errorf("Replication.MinInsyncReplicas = %d, want 2", cfg.Replication.MinInsyncReplicas)
	}

	if cfg.Group.SessionTimeoutMs != 30000 {
		t.Errorf("Group.SessionTimeoutMs = %d, want 30000", cfg.Group.SessionTimeoutMs)
	}
	if cfg.Group.HeartbeatIntervalMs != 5000 {
		t.Errorf("Group.HeartbeatIntervalMs = %d, want 5000", cfg.Group.HeartbeatIntervalMs)
	}
	if cfg.Group.RebalanceTimeoutMs != 45000 {
		t.Errorf("Group.RebalanceTimeoutMs = %d, want 45000", cfg.Group.RebalanceTimeoutMs)
	}
	if cfg.Group.OffsetsTopicPartitions != 25 {
		t.Errorf("Group.OffsetsTopicPartitions = %d, want 25", cfg.Group.OffsetsTopicPartitions)
	}

	if len(cfg.Cluster.Brokers) != 1 {
		t.Fatalf("len(Cluster.Brokers) = %d, want 1", len(cfg.Cluster.Brokers))
	}
	b := cfg.Cluster.Brokers[0]
	if b.ID != 1 || b.Host != "broker1" || b.Port != 9092 {
		t.Errorf("Cluster.Brokers[0] = %+v, want {1 broker1 9092}", b)
	}
}

func TestLoad_olmayan_dosya_icin_hata_doner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded for missing file, want error")
	}
	if !strings.Contains(err.Error(), "read") {
		t.Errorf("error does not mention read: %v", err)
	}
}

func TestLoad_gecersiz_YAML_icin_hata_doner(t *testing.T) {
	path := writeYAML(t, t.TempDir(), "invalid.yaml", "broker: [unclosed")

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load succeeded for invalid YAML, want error")
	}
	if !strings.Contains(err.Error(), "parse") {
		t.Errorf("error does not mention parse: %v", err)
	}
}

func TestWithDefaults_zero_degerli_alanlari_doldurur(t *testing.T) {
	cfg := &Config{}
	cfg.WithDefaults()

	if cfg.Broker.ID != 1 {
		t.Errorf("Broker.ID = %d, want 1", cfg.Broker.ID)
	}
	if cfg.Broker.Host != "0.0.0.0" {
		t.Errorf("Broker.Host = %q, want 0.0.0.0", cfg.Broker.Host)
	}
	if cfg.Broker.Port != 9092 {
		t.Errorf("Broker.Port = %d, want 9092", cfg.Broker.Port)
	}
	if cfg.Broker.DataDir != "/var/lib/mini-kafka" {
		t.Errorf("Broker.DataDir = %q, want /var/lib/mini-kafka", cfg.Broker.DataDir)
	}
	if cfg.Broker.MaxConnections != 1024 {
		t.Errorf("Broker.MaxConnections = %d, want 1024", cfg.Broker.MaxConnections)
	}
	if cfg.Broker.RequestTimeoutMs != 30000 {
		t.Errorf("Broker.RequestTimeoutMs = %d, want 30000", cfg.Broker.RequestTimeoutMs)
	}

	if cfg.Log.SegmentBytes != 128*1024*1024 {
		t.Errorf("Log.SegmentBytes = %d, want 134217728", cfg.Log.SegmentBytes)
	}
	if cfg.Log.SegmentMs != 7*24*60*60*1000 {
		t.Errorf("Log.SegmentMs = %d, want 604800000", cfg.Log.SegmentMs)
	}
	if cfg.Log.IndexIntervalBytes != 4096 {
		t.Errorf("Log.IndexIntervalBytes = %d, want 4096", cfg.Log.IndexIntervalBytes)
	}
	if cfg.Log.IndexMaxBytes != 10*1024*1024 {
		t.Errorf("Log.IndexMaxBytes = %d, want 10485760", cfg.Log.IndexMaxBytes)
	}
	if cfg.Log.RetentionMs != 7*24*60*60*1000 {
		t.Errorf("Log.RetentionMs = %d, want 604800000", cfg.Log.RetentionMs)
	}
	if cfg.Log.RetentionBytes != -1 {
		t.Errorf("Log.RetentionBytes = %d, want -1", cfg.Log.RetentionBytes)
	}
	if cfg.Log.FlushMs != 1000 {
		t.Errorf("Log.FlushMs = %d, want 1000", cfg.Log.FlushMs)
	}
	if cfg.Log.MaxMessageBytes != 1024*1024 {
		t.Errorf("Log.MaxMessageBytes = %d, want 1048576", cfg.Log.MaxMessageBytes)
	}

	if cfg.Topic.DefaultPartitions != 3 {
		t.Errorf("Topic.DefaultPartitions = %d, want 3", cfg.Topic.DefaultPartitions)
	}
	if cfg.Topic.DefaultReplicationFactor != 1 {
		t.Errorf("Topic.DefaultReplicationFactor = %d, want 1", cfg.Topic.DefaultReplicationFactor)
	}

	if cfg.Replication.ReplicaLagTimeMaxMs != 30000 {
		t.Errorf("Replication.ReplicaLagTimeMaxMs = %d, want 30000", cfg.Replication.ReplicaLagTimeMaxMs)
	}
	if cfg.Replication.ReplicaFetchMaxBytes != 1048576 {
		t.Errorf("Replication.ReplicaFetchMaxBytes = %d, want 1048576", cfg.Replication.ReplicaFetchMaxBytes)
	}
	if cfg.Replication.ReplicaFetchWaitMaxMs != 500 {
		t.Errorf("Replication.ReplicaFetchWaitMaxMs = %d, want 500", cfg.Replication.ReplicaFetchWaitMaxMs)
	}
	if cfg.Replication.MinInsyncReplicas != 1 {
		t.Errorf("Replication.MinInsyncReplicas = %d, want 1", cfg.Replication.MinInsyncReplicas)
	}

	if cfg.Group.SessionTimeoutMs != 45000 {
		t.Errorf("Group.SessionTimeoutMs = %d, want 45000", cfg.Group.SessionTimeoutMs)
	}
	if cfg.Group.HeartbeatIntervalMs != 3000 {
		t.Errorf("Group.HeartbeatIntervalMs = %d, want 3000", cfg.Group.HeartbeatIntervalMs)
	}
	if cfg.Group.RebalanceTimeoutMs != 60000 {
		t.Errorf("Group.RebalanceTimeoutMs = %d, want 60000", cfg.Group.RebalanceTimeoutMs)
	}
	if cfg.Group.OffsetsTopicPartitions != 50 {
		t.Errorf("Group.OffsetsTopicPartitions = %d, want 50", cfg.Group.OffsetsTopicPartitions)
	}
}

func TestWithDefaults_zaten_set_edilmis_degerleri_degistirmez(t *testing.T) {
	cfg := &Config{
		Broker: BrokerConfig{
			ID:               7,
			Host:             "127.0.0.1",
			Port:             9093,
			DataDir:          "/custom/data",
			MaxConnections:   500,
			RequestTimeoutMs: 10000,
		},
		Log: LogConfig{
			SegmentBytes:       64 * 1024 * 1024,
			SegmentMs:          24 * 60 * 60 * 1000,
			IndexIntervalBytes: 2048,
			IndexMaxBytes:      5 * 1024 * 1024,
			RetentionMs:        24 * 60 * 60 * 1000,
			RetentionBytes:     -1,
			FlushMs:            500,
			MaxMessageBytes:    512 * 1024,
		},
		Topic: TopicConfig{
			AutoCreate:               true,
			DefaultPartitions:        6,
			DefaultReplicationFactor: 2,
		},
		Replication: ReplicationConfig{
			ReplicaLagTimeMaxMs:   15000,
			ReplicaFetchMaxBytes:  524288,
			ReplicaFetchWaitMaxMs: 250,
			MinInsyncReplicas:     2,
		},
		Group: GroupConfig{
			SessionTimeoutMs:       30000,
			HeartbeatIntervalMs:    5000,
			RebalanceTimeoutMs:     45000,
			OffsetsTopicPartitions: 25,
		},
		Cluster: ClusterConfig{
			Brokers: []ClusterBroker{
				{ID: 99, Host: "custom", Port: 9999},
			},
		},
	}
	want := *cfg

	cfg.WithDefaults()

	if cfg.Broker != want.Broker {
		t.Errorf("Broker changed: got %+v, want %+v", cfg.Broker, want.Broker)
	}
	if cfg.Log != want.Log {
		t.Errorf("Log changed: got %+v, want %+v", cfg.Log, want.Log)
	}
	if cfg.Topic != want.Topic {
		t.Errorf("Topic changed: got %+v, want %+v", cfg.Topic, want.Topic)
	}
	if cfg.Replication != want.Replication {
		t.Errorf("Replication changed: got %+v, want %+v", cfg.Replication, want.Replication)
	}
	if cfg.Group != want.Group {
		t.Errorf("Group changed: got %+v, want %+v", cfg.Group, want.Group)
	}
	if len(cfg.Cluster.Brokers) != len(want.Cluster.Brokers) || cfg.Cluster.Brokers[0] != want.Cluster.Brokers[0] {
		t.Errorf("Cluster changed: got %+v, want %+v", cfg.Cluster, want.Cluster)
	}
}

func TestLoad_tum_alt_config_yapilari_parse_ediliyor(t *testing.T) {
	path := writeYAML(t, t.TempDir(), "full.yaml", validFullYAML())

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if cfg.Broker.ID == 0 {
		t.Errorf("Broker.ID not parsed")
	}
	if cfg.Log.SegmentBytes == 0 {
		t.Errorf("Log.SegmentBytes not parsed")
	}
	if cfg.Topic.DefaultPartitions == 0 {
		t.Errorf("Topic.DefaultPartitions not parsed")
	}
	if cfg.Replication.MinInsyncReplicas == 0 {
		t.Errorf("Replication.MinInsyncReplicas not parsed")
	}
	if cfg.Group.OffsetsTopicPartitions == 0 {
		t.Errorf("Group.OffsetsTopicPartitions not parsed")
	}
	if len(cfg.Cluster.Brokers) == 0 {
		t.Errorf("Cluster.Brokers not parsed")
	}
}
