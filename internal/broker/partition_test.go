package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/storage"
)

func TestPartition_Append_and_Read(t *testing.T) {
	dir := t.TempDir()
	part, err := NewPartition(dir, "test-topic", 0, storage.Config{})
	if err != nil {
		t.Fatalf("NewPartition error: %v", err)
	}
	defer part.Close()

	var recs []*storage.Record
	for i := 0; i < 5; i++ {
		recs = append(recs, &storage.Record{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("val-%d", i)),
		})
	}

	baseOffset, err := part.AppendBatch(recs)
	if err != nil {
		t.Fatalf("AppendBatch error: %v", err)
	}
	if baseOffset != 0 {
		t.Errorf("baseOffset = %d, want 0", baseOffset)
	}

	if part.LogEndOffset() != 5 {
		t.Errorf("LogEndOffset = %d, want 5", part.LogEndOffset())
	}

	readRecs, err := part.ReadFrom(0, 1024*1024)
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if len(readRecs) != 5 {
		t.Fatalf("len(readRecs) = %d, want 5", len(readRecs))
	}
	if string(readRecs[0].Key) != "key-0" || string(readRecs[4].Value) != "val-4" {
		t.Errorf("Record content mismatch")
	}
}
