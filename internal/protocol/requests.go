// Package protocol implements the wire codec for the mini-kafka binary protocol.
//
// This file defines the request body types for the APIs implemented in Faz 2
// (MINI_KAFKA_SPEC.md Section 5.5): Produce (apiKey 0), Fetch (apiKey 1) and
// ApiVersions (apiKey 12). Each type provides Encode and Decode methods that
// use the primitive codec functions defined in codec.go. All multi-byte
// integers are big-endian.
package protocol

import (
	"io"
)

// ProduceRequestPartition is a single partition entry within a ProduceRequest
// topic: the partition identifier and the opaque record set (a sequence of
// records in the Section 4.1 format) to append.
type ProduceRequestPartition struct {
	// PartitionID is the zero-based partition index within the topic.
	PartitionID int32
	// RecordSet is the raw, length-prefixed byte sequence of records to append.
	RecordSet []byte
}

// ProduceRequestTopic is a single topic entry within a ProduceRequest: the
// topic name and the partitions whose record sets should be appended.
type ProduceRequestTopic struct {
	// Name is the topic name.
	Name string
	// Partitions is the list of partition/recordSet pairs to append.
	Partitions []ProduceRequestPartition
}

// ProduceRequest is the body of the Produce API (apiKey 0). It carries the
// producer's acks mode, a server-side timeout and the batched record sets to
// append to one or more topic partitions.
type ProduceRequest struct {
	// Acks controls the durability guarantee requested: 0 (fire and forget),
	// 1 (leader acknowledged) or -1 (all ISR acknowledged).
	Acks int16
	// TimeoutMs is the maximum time, in milliseconds, the server will wait
	// for the requested acks to be met before returning a RequestTimedOut
	// error.
	TimeoutMs int32
	// Topics is the list of topic/partition batches to append.
	Topics []ProduceRequestTopic
}

// Encode writes the ProduceRequest body to w using the primitive codec
// functions. It returns any error from the underlying writer.
func (r *ProduceRequest) Encode(w io.Writer) error {
	if _, err := PutInt16(w, r.Acks); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.TimeoutMs); err != nil {
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutArrayHeader(w, len(t.Partitions)); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			if _, err := PutInt32(w, p.PartitionID); err != nil {
				return err
			}
			if _, err := PutBytes(w, p.RecordSet); err != nil {
				return err
			}
		}
	}
	return nil
}

// Decode reads the ProduceRequest body from r using the primitive codec
// functions. A null topics array is decoded as nil. It returns any error from
// the underlying reader.
func (r *ProduceRequest) Decode(rd io.Reader) error {
	acks, err := Int16(rd)
	if err != nil {
		return err
	}
	r.Acks = acks

	timeoutMs, err := Int32(rd)
	if err != nil {
		return err
	}
	r.TimeoutMs = timeoutMs

	topicCount, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if topicCount < 0 {
		r.Topics = nil
		return nil
	}
	r.Topics = make([]ProduceRequestTopic, topicCount)
	for i := 0; i < topicCount; i++ {
		var t ProduceRequestTopic
		if t.Name, err = String(rd); err != nil {
			return err
		}
		partCount, err := ReadArrayHeader(rd)
		if err != nil {
			return err
		}
		if partCount < 0 {
			t.Partitions = nil
			r.Topics[i] = t
			continue
		}
		t.Partitions = make([]ProduceRequestPartition, partCount)
		for j := 0; j < partCount; j++ {
			var p ProduceRequestPartition
			if p.PartitionID, err = Int32(rd); err != nil {
				return err
			}
			if p.RecordSet, err = Bytes(rd); err != nil {
				return err
			}
			t.Partitions[j] = p
		}
		r.Topics[i] = t
	}
	return nil
}

// FetchRequestPartition is a single partition entry within a FetchRequest
// topic: the partition to read from, the offset to start at and the maximum
// number of bytes to return.
type FetchRequestPartition struct {
	// PartitionID is the zero-based partition index within the topic.
	PartitionID int32
	// FetchOffset is the absolute log offset of the first record to return.
	FetchOffset int64
	// MaxBytes is the maximum number of bytes of record data to return for
	// this partition.
	MaxBytes int32
}

// FetchRequestTopic is a single topic entry within a FetchRequest: the topic
// name and the partitions to read from.
type FetchRequestTopic struct {
	// Name is the topic name.
	Name string
	// Partitions is the list of partition fetch specifications.
	Partitions []FetchRequestPartition
}

// FetchRequest is the body of the Fetch API (apiKey 1). It carries the
// long-poll parameters (max wait, min/max bytes) and the topic/partition
// ranges to read.
type FetchRequest struct {
	// MaxWaitMs is the long-poll duration in milliseconds: the server blocks
	// for at most this long waiting for minBytes to accumulate.
	MaxWaitMs int32
	// MinBytes is the minimum number of bytes of record data the server must
	// accumulate before returning. A value of 0 means return as soon as any
	// data is available.
	MinBytes int32
	// MaxBytes is the maximum total number of bytes of record data the server
	// may return across all partitions.
	MaxBytes int32
	// Topics is the list of topic/partition fetch specifications.
	Topics []FetchRequestTopic
}

// Encode writes the FetchRequest body to w using the primitive codec
// functions. It returns any error from the underlying writer.
func (r *FetchRequest) Encode(w io.Writer) error {
	if _, err := PutInt32(w, r.MaxWaitMs); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.MinBytes); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.MaxBytes); err != nil {
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutArrayHeader(w, len(t.Partitions)); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			if _, err := PutInt32(w, p.PartitionID); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.FetchOffset); err != nil {
				return err
			}
			if _, err := PutInt32(w, p.MaxBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

// Decode reads the FetchRequest body from r using the primitive codec
// functions. A null topics array is decoded as nil. It returns any error from
// the underlying reader.
func (r *FetchRequest) Decode(rd io.Reader) error {
	maxWait, err := Int32(rd)
	if err != nil {
		return err
	}
	r.MaxWaitMs = maxWait

	minBytes, err := Int32(rd)
	if err != nil {
		return err
	}
	r.MinBytes = minBytes

	maxBytes, err := Int32(rd)
	if err != nil {
		return err
	}
	r.MaxBytes = maxBytes

	topicCount, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if topicCount < 0 {
		r.Topics = nil
		return nil
	}
	r.Topics = make([]FetchRequestTopic, topicCount)
	for i := 0; i < topicCount; i++ {
		var t FetchRequestTopic
		if t.Name, err = String(rd); err != nil {
			return err
		}
		partCount, err := ReadArrayHeader(rd)
		if err != nil {
			return err
		}
		if partCount < 0 {
			t.Partitions = nil
			r.Topics[i] = t
			continue
		}
		t.Partitions = make([]FetchRequestPartition, partCount)
		for j := 0; j < partCount; j++ {
			var p FetchRequestPartition
			if p.PartitionID, err = Int32(rd); err != nil {
				return err
			}
			if p.FetchOffset, err = Int64(rd); err != nil {
				return err
			}
			if p.MaxBytes, err = Int32(rd); err != nil {
				return err
			}
			t.Partitions[j] = p
		}
		r.Topics[i] = t
	}
	return nil
}

// ApiVersionsRequest is the body of the ApiVersions API (apiKey 12). It
// carries no fields; a client sends an empty body to ask the server which API
// keys and version ranges it supports.
type ApiVersionsRequest struct{}

// Encode writes the ApiVersionsRequest body to w. As the request has no
// fields, it writes nothing and never returns an error.
func (r *ApiVersionsRequest) Encode(w io.Writer) error {
	_ = w
	return nil
}

// Decode reads the ApiVersionsRequest body from r. As the request has no
// fields, it consumes nothing and never returns an error.
func (r *ApiVersionsRequest) Decode(rd io.Reader) error {
	_ = rd
	return nil
}

// MetadataRequest is the body of the Metadata API (apiKey 2).
// If Topics is nil (encoded as array count -1), metadata for all topics is requested.
type MetadataRequest struct {
	Topics []string
}

// Encode writes the MetadataRequest body to w.
func (r *MetadataRequest) Encode(w io.Writer) error {
	if r.Topics == nil {
		_, err := PutArrayHeader(w, -1)
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, topic := range r.Topics {
		if _, err := PutString(w, topic); err != nil {
			return err
		}
	}
	return nil
}

// Decode reads the MetadataRequest body from rd.
func (r *MetadataRequest) Decode(rd io.Reader) error {
	count, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if count < 0 {
		r.Topics = nil
		return nil
	}
	r.Topics = make([]string, count)
	for i := 0; i < count; i++ {
		str, err := String(rd)
		if err != nil {
			return err
		}
		r.Topics[i] = str
	}
	return nil
}

// CreateTopicsRequestTopic represents a topic specification in CreateTopicsRequest.
type CreateTopicsRequestTopic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

// CreateTopicsRequest is the body of the CreateTopics API (apiKey 3).
type CreateTopicsRequest struct {
	Topics []CreateTopicsRequestTopic
}

// Encode writes the CreateTopicsRequest body to w.
func (r *CreateTopicsRequest) Encode(w io.Writer) error {
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutInt32(w, t.NumPartitions); err != nil {
			return err
		}
		if _, err := PutInt16(w, t.ReplicationFactor); err != nil {
			return err
		}
	}
	return nil
}

// Decode reads the CreateTopicsRequest body from rd.
func (r *CreateTopicsRequest) Decode(rd io.Reader) error {
	count, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if count < 0 {
		r.Topics = nil
		return nil
	}
	r.Topics = make([]CreateTopicsRequestTopic, count)
	for i := 0; i < count; i++ {
		var t CreateTopicsRequestTopic
		var err error
		if t.Name, err = String(rd); err != nil {
			return err
		}
		if t.NumPartitions, err = Int32(rd); err != nil {
			return err
		}
		if t.ReplicationFactor, err = Int16(rd); err != nil {
			return err
		}
		r.Topics[i] = t
	}
	return nil
}

// JoinGroupProtocol represents a consumer group protocol choice in JoinGroupRequest.
type JoinGroupProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupRequest (apiKey 4)
type JoinGroupRequest struct {
	GroupID          string
	SessionTimeoutMs int32
	MemberID         string
	ProtocolType     string
	Protocols        []JoinGroupProtocol
}

func (r *JoinGroupRequest) Encode(w io.Writer) error {
	if _, err := PutString(w, r.GroupID); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.SessionTimeoutMs); err != nil {
		return err
	}
	if _, err := PutString(w, r.MemberID); err != nil {
		return err
	}
	if _, err := PutString(w, r.ProtocolType); err != nil {
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Protocols)); err != nil {
		return err
	}
	for _, p := range r.Protocols {
		if _, err := PutString(w, p.Name); err != nil {
			return err
		}
		if _, err := PutBytes(w, p.Metadata); err != nil {
			return err
		}
	}
	return nil
}

func (r *JoinGroupRequest) Decode(rd io.Reader) error {
	var err error
	if r.GroupID, err = String(rd); err != nil {
		return err
	}
	if r.SessionTimeoutMs, err = Int32(rd); err != nil {
		return err
	}
	if r.MemberID, err = String(rd); err != nil {
		return err
	}
	if r.ProtocolType, err = String(rd); err != nil {
		return err
	}
	count, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if count >= 0 {
		r.Protocols = make([]JoinGroupProtocol, count)
		for i := 0; i < count; i++ {
			var p JoinGroupProtocol
			if p.Name, err = String(rd); err != nil {
				return err
			}
			if p.Metadata, err = Bytes(rd); err != nil {
				return err
			}
			r.Protocols[i] = p
		}
	}
	return nil
}

// SyncGroupAssignment represents a member's assigned partitions payload.
type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupRequest (apiKey 5)
type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []SyncGroupAssignment
}

func (r *SyncGroupRequest) Encode(w io.Writer) error {
	if _, err := PutString(w, r.GroupID); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.GenerationID); err != nil {
		return err
	}
	if _, err := PutString(w, r.MemberID); err != nil {
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Assignments)); err != nil {
		return err
	}
	for _, a := range r.Assignments {
		if _, err := PutString(w, a.MemberID); err != nil {
			return err
		}
		if _, err := PutBytes(w, a.Assignment); err != nil {
			return err
		}
	}
	return nil
}

func (r *SyncGroupRequest) Decode(rd io.Reader) error {
	var err error
	if r.GroupID, err = String(rd); err != nil {
		return err
	}
	if r.GenerationID, err = Int32(rd); err != nil {
		return err
	}
	if r.MemberID, err = String(rd); err != nil {
		return err
	}
	count, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if count >= 0 {
		r.Assignments = make([]SyncGroupAssignment, count)
		for i := 0; i < count; i++ {
			var a SyncGroupAssignment
			if a.MemberID, err = String(rd); err != nil {
				return err
			}
			if a.Assignment, err = Bytes(rd); err != nil {
				return err
			}
			r.Assignments[i] = a
		}
	}
	return nil
}

// HeartbeatRequest (apiKey 6)
type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

func (r *HeartbeatRequest) Encode(w io.Writer) error {
	if _, err := PutString(w, r.GroupID); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.GenerationID); err != nil {
		return err
	}
	if _, err := PutString(w, r.MemberID); err != nil {
		return err
	}
	return nil
}

func (r *HeartbeatRequest) Decode(rd io.Reader) error {
	var err error
	if r.GroupID, err = String(rd); err != nil {
		return err
	}
	if r.GenerationID, err = Int32(rd); err != nil {
		return err
	}
	if r.MemberID, err = String(rd); err != nil {
		return err
	}
	return nil
}

// LeaveGroupRequest (apiKey 7)
type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

func (r *LeaveGroupRequest) Encode(w io.Writer) error {
	if _, err := PutString(w, r.GroupID); err != nil {
		return err
	}
	if _, err := PutString(w, r.MemberID); err != nil {
		return err
	}
	return nil
}

func (r *LeaveGroupRequest) Decode(rd io.Reader) error {
	var err error
	if r.GroupID, err = String(rd); err != nil {
		return err
	}
	if r.MemberID, err = String(rd); err != nil {
		return err
	}
	return nil
}

// OffsetCommitRequestPartition represents partition offset info in OffsetCommitRequest.
type OffsetCommitRequestPartition struct {
	PartitionID int32
	Offset      int64
	Metadata    string
}

// OffsetCommitRequestTopic represents topic info in OffsetCommitRequest.
type OffsetCommitRequestTopic struct {
	Name       string
	Partitions []OffsetCommitRequestPartition
}

// OffsetCommitRequest (apiKey 8)
type OffsetCommitRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Topics       []OffsetCommitRequestTopic
}

func (r *OffsetCommitRequest) Encode(w io.Writer) error {
	if _, err := PutString(w, r.GroupID); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.GenerationID); err != nil {
		return err
	}
	if _, err := PutString(w, r.MemberID); err != nil {
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutArrayHeader(w, len(t.Partitions)); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			if _, err := PutInt32(w, p.PartitionID); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.Offset); err != nil {
				return err
			}
			if _, err := PutString(w, p.Metadata); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetCommitRequest) Decode(rd io.Reader) error {
	var err error
	if r.GroupID, err = String(rd); err != nil {
		return err
	}
	if r.GenerationID, err = Int32(rd); err != nil {
		return err
	}
	if r.MemberID, err = String(rd); err != nil {
		return err
	}
	tCount, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if tCount >= 0 {
		r.Topics = make([]OffsetCommitRequestTopic, tCount)
		for i := 0; i < tCount; i++ {
			var t OffsetCommitRequestTopic
			if t.Name, err = String(rd); err != nil {
				return err
			}
			pCount, err := ReadArrayHeader(rd)
			if err != nil {
				return err
			}
			if pCount >= 0 {
				t.Partitions = make([]OffsetCommitRequestPartition, pCount)
				for j := 0; j < pCount; j++ {
					var p OffsetCommitRequestPartition
					if p.PartitionID, err = Int32(rd); err != nil {
						return err
					}
					if p.Offset, err = Int64(rd); err != nil {
						return err
					}
					if p.Metadata, err = String(rd); err != nil {
						return err
					}
					t.Partitions[j] = p
				}
			}
			r.Topics[i] = t
		}
	}
	return nil
}

// OffsetFetchRequestTopic represents topic info in OffsetFetchRequest.
type OffsetFetchRequestTopic struct {
	Name       string
	Partitions []int32
}

// OffsetFetchRequest (apiKey 9)
type OffsetFetchRequest struct {
	GroupID string
	Topics  []OffsetFetchRequestTopic
}

func (r *OffsetFetchRequest) Encode(w io.Writer) error {
	if _, err := PutString(w, r.GroupID); err != nil {
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutArrayHeader(w, len(t.Partitions)); err != nil {
			return err
		}
		for _, pID := range t.Partitions {
			if _, err := PutInt32(w, pID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetFetchRequest) Decode(rd io.Reader) error {
	var err error
	if r.GroupID, err = String(rd); err != nil {
		return err
	}
	tCount, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if tCount >= 0 {
		r.Topics = make([]OffsetFetchRequestTopic, tCount)
		for i := 0; i < tCount; i++ {
			var t OffsetFetchRequestTopic
			if t.Name, err = String(rd); err != nil {
				return err
			}
			pCount, err := ReadArrayHeader(rd)
			if err != nil {
				return err
			}
			if pCount >= 0 {
				t.Partitions = make([]int32, pCount)
				for j := 0; j < pCount; j++ {
					if t.Partitions[j], err = Int32(rd); err != nil {
						return err
					}
				}
			}
			r.Topics[i] = t
		}
	}
	return nil
}

// ListOffsetsRequestPartition represents partition target in ListOffsetsRequest.
type ListOffsetsRequestPartition struct {
	PartitionID int32
	Timestamp   int64 // -1 = latest, -2 = earliest
}

// ListOffsetsRequestTopic represents topic in ListOffsetsRequest.
type ListOffsetsRequestTopic struct {
	Name       string
	Partitions []ListOffsetsRequestPartition
}

// ListOffsetsRequest (apiKey 10)
type ListOffsetsRequest struct {
	Topics []ListOffsetsRequestTopic
}

func (r *ListOffsetsRequest) Encode(w io.Writer) error {
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutArrayHeader(w, len(t.Partitions)); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			if _, err := PutInt32(w, p.PartitionID); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.Timestamp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ListOffsetsRequest) Decode(rd io.Reader) error {
	tCount, err := ReadArrayHeader(rd)
	if err != nil {
		return err
	}
	if tCount >= 0 {
		r.Topics = make([]ListOffsetsRequestTopic, tCount)
		for i := 0; i < tCount; i++ {
			var t ListOffsetsRequestTopic
			var err error
			if t.Name, err = String(rd); err != nil {
				return err
			}
			pCount, err := ReadArrayHeader(rd)
			if err != nil {
				return err
			}
			if pCount >= 0 {
				t.Partitions = make([]ListOffsetsRequestPartition, pCount)
				for j := 0; j < pCount; j++ {
					var p ListOffsetsRequestPartition
					if p.PartitionID, err = Int32(rd); err != nil {
						return err
					}
					if p.Timestamp, err = Int64(rd); err != nil {
						return err
					}
					t.Partitions[j] = p
				}
			}
			r.Topics[i] = t
		}
	}
	return nil
}
