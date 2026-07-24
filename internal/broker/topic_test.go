package broker

import (
	"testing"

	"github.com/YusuffEren/mini-kafka/internal/storage"
)

func TestMurmur2_Kafka_Test_Vectors(t *testing.T) {
	tests := []struct {
		input    string
		wantHash uint32
	}{
		{"", 590343075},
		{"a", 242020996},
		{"test", 146065218},
		{"kafka", 1519210584},
		{"mini-kafka", 1423008314},
	}

	for _, tt := range tests {
		got := Murmur2([]byte(tt.input))
		if got != tt.wantHash {
			t.Errorf("Murmur2(%q) = %d (0x%x), want %d (0x%x)", tt.input, got, got, tt.wantHash, tt.wantHash)
		}
	}
}

func TestTopic_PartitionFor(t *testing.T) {
	dir := t.TempDir()
	topic, err := NewTopic(dir, "my-topic", 4, storage.Config{})
	if err != nil {
		t.Fatalf("NewTopic error: %v", err)
	}
	defer topic.Close()

	// Same key should always map to same partition
	key := []byte("user-12345")
	p1 := topic.PartitionFor(key)
	p2 := topic.PartitionFor(key)
	if p1 != p2 {
		t.Errorf("PartitionFor deterministic check failed: %d vs %d", p1, p2)
	}
	if p1 < 0 || p1 >= 4 {
		t.Errorf("PartitionFor out of bounds: %d", p1)
	}

	// Nil key should round-robin across 4 partitions
	counts := make(map[int32]int)
	for i := 0; i < 40; i++ {
		p := topic.PartitionFor(nil)
		counts[p]++
	}
	for i := int32(0); i < 4; i++ {
		if counts[i] != 10 {
			t.Errorf("RoundRobin count for partition %d = %d, want 10", i, counts[i])
		}
	}
}
