package protocol

import (
	"bytes"
	"io"
	"testing"
)

// ---------------------------------------------------------------------------
// ProduceRequest tests
// ---------------------------------------------------------------------------

func TestProduceRequest_roundTrip_emptyTopics(t *testing.T) {
	want := ProduceRequest{
		Acks:      1,
		TimeoutMs: 30000,
		Topics:    []ProduceRequestTopic{},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ProduceRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Acks != want.Acks {
		t.Errorf("Acks: got %d, want %d", got.Acks, want.Acks)
	}
	if got.TimeoutMs != want.TimeoutMs {
		t.Errorf("TimeoutMs: got %d, want %d", got.TimeoutMs, want.TimeoutMs)
	}
	if len(got.Topics) != 0 {
		t.Errorf("Topics length: got %d, want 0", len(got.Topics))
	}
}

func TestProduceRequest_roundTrip_singleTopicSinglePartition(t *testing.T) {
	want := ProduceRequest{
		Acks:      -1,
		TimeoutMs: 5000,
		Topics: []ProduceRequestTopic{
			{
				Name: "events",
				Partitions: []ProduceRequestPartition{
					{PartitionID: 0, RecordSet: []byte("hello")},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ProduceRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !produceRequestsEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestProduceRequest_roundTrip_multipleTopicsMultiplePartitions(t *testing.T) {
	want := ProduceRequest{
		Acks:      1,
		TimeoutMs: 10000,
		Topics: []ProduceRequestTopic{
			{
				Name: "topic-a",
				Partitions: []ProduceRequestPartition{
					{PartitionID: 0, RecordSet: []byte("record-a0")},
					{PartitionID: 1, RecordSet: []byte("record-a1")},
				},
			},
			{
				Name: "topic-b",
				Partitions: []ProduceRequestPartition{
					{PartitionID: 0, RecordSet: []byte("record-b0")},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ProduceRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !produceRequestsEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestProduceRequest_roundTrip_binaryRecordSet(t *testing.T) {
	want := ProduceRequest{
		Acks:      0,
		TimeoutMs: 1000,
		Topics: []ProduceRequestTopic{
			{
				Name: "binary",
				Partitions: []ProduceRequestPartition{
					{
						PartitionID: 7,
						RecordSet:   []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ProduceRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !produceRequestsEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestProduceRequest_decode_nullTopicsArray(t *testing.T) {
	// Acks(2) + TimeoutMs(4) + ArrayHeader(-1)
	data := []byte{0x00, 0x01, 0x00, 0x00, 0x0b, 0xb8, 0xff, 0xff, 0xff, 0xff}

	var got ProduceRequest
	if err := got.Decode(bytes.NewReader(data)); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Topics != nil {
		t.Errorf("Topics: got %v, want nil", got.Topics)
	}
}

func TestProduceRequest_decode_truncatedAfterAcks(t *testing.T) {
	data := []byte{0x00, 0x01} // only Acks, missing TimeoutMs

	var got ProduceRequest
	err := got.Decode(bytes.NewReader(data))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestProduceRequest_decode_emptyReader(t *testing.T) {
	var got ProduceRequest
	err := got.Decode(bytes.NewReader(nil))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestProduceRequest_decode_truncatedTopicString(t *testing.T) {
	// Acks(2) + TimeoutMs(4) + ArrayHeader(1) + string length(2) but no bytes
	data := []byte{
		0x00, 0x01,
		0x00, 0x00, 0x0b, 0xb8,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05,
	}

	var got ProduceRequest
	err := got.Decode(bytes.NewReader(data))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestProduceRequest_decode_partialTopicString(t *testing.T) {
	// Acks(2) + TimeoutMs(4) + ArrayHeader(1) + string length(5) + only 2 string bytes
	data := []byte{
		0x00, 0x01,
		0x00, 0x00, 0x0b, 0xb8,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05,
		0x68, 0x65,
	}

	var got ProduceRequest
	err := got.Decode(bytes.NewReader(data))
	if err != io.ErrUnexpectedEOF {
		t.Errorf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func produceRequestsEqual(a, b ProduceRequest) bool {
	if a.Acks != b.Acks || a.TimeoutMs != b.TimeoutMs || len(a.Topics) != len(b.Topics) {
		return false
	}
	for i := range a.Topics {
		if a.Topics[i].Name != b.Topics[i].Name || len(a.Topics[i].Partitions) != len(b.Topics[i].Partitions) {
			return false
		}
		for j := range a.Topics[i].Partitions {
			if a.Topics[i].Partitions[j].PartitionID != b.Topics[i].Partitions[j].PartitionID {
				return false
			}
			if !bytes.Equal(a.Topics[i].Partitions[j].RecordSet, b.Topics[i].Partitions[j].RecordSet) {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// FetchRequest tests
// ---------------------------------------------------------------------------

func TestFetchRequest_roundTrip_emptyTopics(t *testing.T) {
	want := FetchRequest{
		MaxWaitMs: 500,
		MinBytes:  1,
		MaxBytes:  1048576,
		Topics:    []FetchRequestTopic{},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got FetchRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !fetchRequestsEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestFetchRequest_roundTrip_singlePartition(t *testing.T) {
	want := FetchRequest{
		MaxWaitMs: 1000,
		MinBytes:  0,
		MaxBytes:  52428800,
		Topics: []FetchRequestTopic{
			{
				Name: "events",
				Partitions: []FetchRequestPartition{
					{PartitionID: 0, FetchOffset: 42, MaxBytes: 1024},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got FetchRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !fetchRequestsEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestFetchRequest_roundTrip_maxBytesBoundary(t *testing.T) {
	want := FetchRequest{
		MaxWaitMs: 0,
		MinBytes:  0,
		MaxBytes:  2147483647, // math.MaxInt32
		Topics: []FetchRequestTopic{
			{
				Name: "max-bytes-topic",
				Partitions: []FetchRequestPartition{
					{PartitionID: 0, FetchOffset: 0, MaxBytes: 2147483647},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got FetchRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !fetchRequestsEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestFetchRequest_decode_nullTopicsArray(t *testing.T) {
	// MaxWaitMs(4) + MinBytes(4) + MaxBytes(4) + ArrayHeader(-1)
	data := []byte{
		0x00, 0x00, 0x01, 0xf4,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x10, 0x00, 0x00,
		0xff, 0xff, 0xff, 0xff,
	}

	var got FetchRequest
	if err := got.Decode(bytes.NewReader(data)); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Topics != nil {
		t.Errorf("Topics: got %v, want nil", got.Topics)
	}
}

func TestFetchRequest_decode_truncatedAfterHeader(t *testing.T) {
	data := []byte{
		0x00, 0x00, 0x01, 0xf4,
		0x00, 0x00, 0x00, 0x01,
		// missing MaxBytes
	}

	var got FetchRequest
	err := got.Decode(bytes.NewReader(data))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestFetchRequest_decode_emptyReader(t *testing.T) {
	var got FetchRequest
	err := got.Decode(bytes.NewReader(nil))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func fetchRequestsEqual(a, b FetchRequest) bool {
	if a.MaxWaitMs != b.MaxWaitMs || a.MinBytes != b.MinBytes || a.MaxBytes != b.MaxBytes || len(a.Topics) != len(b.Topics) {
		return false
	}
	for i := range a.Topics {
		if a.Topics[i].Name != b.Topics[i].Name || len(a.Topics[i].Partitions) != len(b.Topics[i].Partitions) {
			return false
		}
		for j := range a.Topics[i].Partitions {
			pa := a.Topics[i].Partitions[j]
			pb := b.Topics[i].Partitions[j]
			if pa.PartitionID != pb.PartitionID || pa.FetchOffset != pb.FetchOffset || pa.MaxBytes != pb.MaxBytes {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// ApiVersionsRequest tests
// ---------------------------------------------------------------------------

func TestApiVersionsRequest_roundTrip_emptyBody(t *testing.T) {
	want := ApiVersionsRequest{}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("encoded body length: got %d, want 0", buf.Len())
	}

	var got ApiVersionsRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
}

func TestApiVersionsRequest_decode_ignoresExtraBytes(t *testing.T) {
	// ApiVersionsRequest.Decode consumes nothing, so extra bytes are ignored.
	want := ApiVersionsRequest{}
	buf := bytes.NewBuffer([]byte{0x00, 0x01, 0x02, 0x03})

	var got ApiVersionsRequest
	if err := got.Decode(buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestApiVersionsRequest_decode_emptyReader(t *testing.T) {
	var got ApiVersionsRequest
	if err := got.Decode(bytes.NewReader(nil)); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// MetadataRequest & CreateTopicsRequest tests
// ---------------------------------------------------------------------------

func TestMetadataRequest_roundTrip(t *testing.T) {
	want := MetadataRequest{
		Topics: []string{"topic-1", "topic-2"},
	}
	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	var got MetadataRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(got.Topics) != len(want.Topics) {
		t.Fatalf("Topics length: got %d, want %d", len(got.Topics), len(want.Topics))
	}
	for i := range want.Topics {
		if got.Topics[i] != want.Topics[i] {
			t.Errorf("Topic[%d]: got %s, want %s", i, got.Topics[i], want.Topics[i])
		}
	}

	// Test null topics array (requests all topics)
	wantNull := MetadataRequest{Topics: nil}
	buf.Reset()
	if err := wantNull.Encode(&buf); err != nil {
		t.Fatalf("encode null failed: %v", err)
	}
	var gotNull MetadataRequest
	if err := gotNull.Decode(&buf); err != nil {
		t.Fatalf("decode null failed: %v", err)
	}
	if gotNull.Topics != nil {
		t.Errorf("expected nil topics for all-topics request, got %v", gotNull.Topics)
	}
}

func TestCreateTopicsRequest_roundTrip(t *testing.T) {
	want := CreateTopicsRequest{
		Topics: []CreateTopicsRequestTopic{
			{Name: "topic-a", NumPartitions: 3, ReplicationFactor: 1},
			{Name: "topic-b", NumPartitions: 1, ReplicationFactor: 1},
		},
	}
	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	var got CreateTopicsRequest
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(got.Topics) != len(want.Topics) {
		t.Fatalf("Topics length: got %d, want %d", len(got.Topics), len(want.Topics))
	}
	for i := range want.Topics {
		if got.Topics[i] != want.Topics[i] {
			t.Errorf("Topic[%d]: got %+v, want %+v", i, got.Topics[i], want.Topics[i])
		}
	}
}
