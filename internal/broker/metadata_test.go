package broker

import (
	"testing"
)

func TestMetadataManager_Create_and_Persist(t *testing.T) {
	dir := t.TempDir()

	mm1, err := NewMetadataManager(dir)
	if err != nil {
		t.Fatalf("NewMetadataManager 1 error: %v", err)
	}

	meta, created, err := mm1.CreateTopic("orders", 3, 1)
	if err != nil {
		t.Fatalf("CreateTopic error: %v", err)
	}
	if !created {
		t.Fatal("expected created=true")
	}
	if meta.NumPartitions != 3 {
		t.Errorf("NumPartitions = %d, want 3", meta.NumPartitions)
	}

	// Creating duplicate topic returns created=false
	_, created2, err := mm1.CreateTopic("orders", 3, 1)
	if err != nil {
		t.Fatalf("CreateTopic duplicate error: %v", err)
	}
	if created2 {
		t.Fatal("expected created=false for duplicate topic")
	}

	// Reload metadata manager from disk
	mm2, err := NewMetadataManager(dir)
	if err != nil {
		t.Fatalf("NewMetadataManager 2 error: %v", err)
	}

	loadedMeta, exists := mm2.GetTopic("orders")
	if !exists {
		t.Fatal("topic 'orders' not found after reload")
	}
	if loadedMeta.NumPartitions != 3 {
		t.Errorf("loaded NumPartitions = %d, want 3", loadedMeta.NumPartitions)
	}
}
