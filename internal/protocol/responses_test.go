package protocol

import (
	"bytes"
	"io"
	"testing"
)

// ---------------------------------------------------------------------------
// ProduceResponse tests
// ---------------------------------------------------------------------------

func TestProduceResponse_roundTrip_success(t *testing.T) {
	want := ProduceResponse{
		Topics: []ProduceResponseTopic{
			{
				Name: "events",
				Partitions: []ProduceResponsePartition{
					{PartitionID: 0, ErrorCode: 0, BaseOffset: 100, LogAppendTime: 1234567890},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ProduceResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !produceResponsesEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestProduceResponse_roundTrip_errorCode(t *testing.T) {
	want := ProduceResponse{
		Topics: []ProduceResponseTopic{
			{
				Name: "events",
				Partitions: []ProduceResponsePartition{
					{PartitionID: 0, ErrorCode: 3, BaseOffset: -1, LogAppendTime: -1},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ProduceResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !produceResponsesEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestProduceResponse_roundTrip_emptyTopics(t *testing.T) {
	want := ProduceResponse{Topics: []ProduceResponseTopic{}}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ProduceResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(got.Topics) != 0 {
		t.Errorf("Topics length: got %d, want 0", len(got.Topics))
	}
}

func TestProduceResponse_decode_nullTopicsArray(t *testing.T) {
	data := []byte{0xff, 0xff, 0xff, 0xff}

	var got ProduceResponse
	if err := got.Decode(bytes.NewReader(data)); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Topics != nil {
		t.Errorf("Topics: got %v, want nil", got.Topics)
	}
}

func TestProduceResponse_decode_truncatedAfterTopicNameLength(t *testing.T) {
	// ArrayHeader(1) + string length(2) but no further bytes
	data := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05,
	}

	var got ProduceResponse
	err := got.Decode(bytes.NewReader(data))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestProduceResponse_decode_partialTopicName(t *testing.T) {
	// ArrayHeader(1) + string length(5) + only 2 string bytes
	data := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05,
		0x68, 0x65,
	}

	var got ProduceResponse
	err := got.Decode(bytes.NewReader(data))
	if err != io.ErrUnexpectedEOF {
		t.Errorf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestProduceResponse_decode_emptyReader(t *testing.T) {
	var got ProduceResponse
	err := got.Decode(bytes.NewReader(nil))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func produceResponsesEqual(a, b ProduceResponse) bool {
	if len(a.Topics) != len(b.Topics) {
		return false
	}
	for i := range a.Topics {
		if a.Topics[i].Name != b.Topics[i].Name || len(a.Topics[i].Partitions) != len(b.Topics[i].Partitions) {
			return false
		}
		for j := range a.Topics[i].Partitions {
			pa := a.Topics[i].Partitions[j]
			pb := b.Topics[i].Partitions[j]
			if pa.PartitionID != pb.PartitionID || pa.ErrorCode != pb.ErrorCode || pa.BaseOffset != pb.BaseOffset || pa.LogAppendTime != pb.LogAppendTime {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// FetchResponse tests
// ---------------------------------------------------------------------------

func TestFetchResponse_roundTrip_withRecords(t *testing.T) {
	want := FetchResponse{
		Topics: []FetchResponseTopic{
			{
				Name: "events",
				Partitions: []FetchResponsePartition{
					{
						PartitionID:    0,
						ErrorCode:      0,
						HighWatermark:  1000,
						LogStartOffset: 0,
						RecordSet:      []byte("record-batch-data"),
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got FetchResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !fetchResponsesEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestFetchResponse_roundTrip_emptyRecordSet(t *testing.T) {
	want := FetchResponse{
		Topics: []FetchResponseTopic{
			{
				Name: "events",
				Partitions: []FetchResponsePartition{
					{
						PartitionID:    0,
						ErrorCode:      0,
						HighWatermark:  0,
						LogStartOffset: 0,
						RecordSet:      []byte{},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got FetchResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !fetchResponsesEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestFetchResponse_roundTrip_error(t *testing.T) {
	want := FetchResponse{
		Topics: []FetchResponseTopic{
			{
				Name: "events",
				Partitions: []FetchResponsePartition{
					{
						PartitionID:    1,
						ErrorCode:      1,
						HighWatermark:  -1,
						LogStartOffset: -1,
						RecordSet:      nil,
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got FetchResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !fetchResponsesEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestFetchResponse_decode_nullTopicsArray(t *testing.T) {
	data := []byte{0xff, 0xff, 0xff, 0xff}

	var got FetchResponse
	if err := got.Decode(bytes.NewReader(data)); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Topics != nil {
		t.Errorf("Topics: got %v, want nil", got.Topics)
	}
}

func TestFetchResponse_decode_truncatedInPartition(t *testing.T) {
	// ArrayHeader(1) + topic name "x" (2+1) + ArrayHeader(1) + partitionID(4) + errorCode(2) + missing rest
	data := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x01, 'x',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
	}

	var got FetchResponse
	err := got.Decode(bytes.NewReader(data))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestFetchResponse_decode_emptyReader(t *testing.T) {
	var got FetchResponse
	err := got.Decode(bytes.NewReader(nil))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func fetchResponsesEqual(a, b FetchResponse) bool {
	if len(a.Topics) != len(b.Topics) {
		return false
	}
	for i := range a.Topics {
		if a.Topics[i].Name != b.Topics[i].Name || len(a.Topics[i].Partitions) != len(b.Topics[i].Partitions) {
			return false
		}
		for j := range a.Topics[i].Partitions {
			pa := a.Topics[i].Partitions[j]
			pb := b.Topics[i].Partitions[j]
			if pa.PartitionID != pb.PartitionID || pa.ErrorCode != pb.ErrorCode || pa.HighWatermark != pb.HighWatermark || pa.LogStartOffset != pb.LogStartOffset {
				return false
			}
			if !bytes.Equal(pa.RecordSet, pb.RecordSet) {
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// ApiVersionsResponse tests
// ---------------------------------------------------------------------------

func TestApiVersionsResponse_roundTrip_multipleKeys(t *testing.T) {
	want := ApiVersionsResponse{
		ApiKeys: []ApiVersion{
			{ApiKey: 0, MinVersion: 0, MaxVersion: 2},
			{ApiKey: 1, MinVersion: 0, MaxVersion: 4},
			{ApiKey: 12, MinVersion: 0, MaxVersion: 1},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ApiVersionsResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !apiVersionsResponsesEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestApiVersionsResponse_roundTrip_emptyList(t *testing.T) {
	want := ApiVersionsResponse{ApiKeys: []ApiVersion{}}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got ApiVersionsResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(got.ApiKeys) != 0 {
		t.Errorf("ApiKeys length: got %d, want 0", len(got.ApiKeys))
	}
}

func TestApiVersionsResponse_decode_nullArray(t *testing.T) {
	data := []byte{0xff, 0xff, 0xff, 0xff}

	var got ApiVersionsResponse
	if err := got.Decode(bytes.NewReader(data)); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.ApiKeys != nil {
		t.Errorf("ApiKeys: got %v, want nil", got.ApiKeys)
	}
}

func TestApiVersionsResponse_decode_truncatedArray(t *testing.T) {
	// ArrayHeader(1) + partial ApiVersion entry
	data := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00}

	var got ApiVersionsResponse
	err := got.Decode(bytes.NewReader(data))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestApiVersionsResponse_decode_emptyReader(t *testing.T) {
	var got ApiVersionsResponse
	err := got.Decode(bytes.NewReader(nil))
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func apiVersionsResponsesEqual(a, b ApiVersionsResponse) bool {
	if len(a.ApiKeys) != len(b.ApiKeys) {
		return false
	}
	for i := range a.ApiKeys {
		if a.ApiKeys[i] != b.ApiKeys[i] {
			return false
		}
	}
	return true
}

func TestMetadataResponse_roundTrip(t *testing.T) {
	want := MetadataResponse{
		Brokers: []BrokerMetadata{
			{NodeID: 0, Host: "localhost", Port: 9092},
		},
		Topics: []TopicMetadata{
			{
				Name:      "test-topic",
				ErrorCode: 0,
				Partitions: []PartitionMetadata{
					{PartitionID: 0, Leader: 0, Replicas: []int32{0}, ISR: []int32{0}},
					{PartitionID: 1, Leader: 0, Replicas: []int32{0}, ISR: []int32{0}},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got MetadataResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(got.Brokers) != len(want.Brokers) || got.Brokers[0] != want.Brokers[0] {
		t.Errorf("Brokers mismatch: got %+v, want %+v", got.Brokers, want.Brokers)
	}
	if len(got.Topics) != len(want.Topics) {
		t.Fatalf("Topics length mismatch: got %d, want %d", len(got.Topics), len(want.Topics))
	}
	gt := got.Topics[0]
	wt := want.Topics[0]
	if gt.Name != wt.Name || gt.ErrorCode != wt.ErrorCode || len(gt.Partitions) != len(wt.Partitions) {
		t.Fatalf("Topic mismatch: got %+v, want %+v", gt, wt)
	}
	for i := range wt.Partitions {
		gp := gt.Partitions[i]
		wp := wt.Partitions[i]
		if gp.PartitionID != wp.PartitionID || gp.Leader != wp.Leader {
			t.Errorf("Partition[%d] mismatch: got %+v, want %+v", i, gp, wp)
		}
	}
}

func TestCreateTopicsResponse_roundTrip(t *testing.T) {
	want := CreateTopicsResponse{
		Topics: []CreateTopicResponseTopic{
			{Name: "topic-1", ErrorCode: 0},
			{Name: "topic-2", ErrorCode: 14}, // TopicAlreadyExists
		},
	}

	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var got CreateTopicsResponse
	if err := got.Decode(&buf); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(got.Topics) != len(want.Topics) {
		t.Fatalf("Topics length mismatch: got %d, want %d", len(got.Topics), len(want.Topics))
	}
	for i := range want.Topics {
		if got.Topics[i] != want.Topics[i] {
			t.Errorf("Topic[%d] mismatch: got %+v, want %+v", i, got.Topics[i], want.Topics[i])
		}
	}
}
