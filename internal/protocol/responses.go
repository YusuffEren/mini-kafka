// Package protocol implements the wire codec for the mini-kafka binary protocol.
//
// This file defines the response body types for the APIs implemented in Faz 2
// (docs/PROTOCOL.md Section 4): Produce (apiKey 0), Fetch (apiKey 1) and
// ApiVersions (apiKey 12). Each type provides Encode and Decode methods that
// use the primitive codec functions defined in codec.go. All multi-byte
// integers are big-endian.
package protocol

import (
	"io"
)

// ProduceResponsePartition is a single partition entry within a
// ProduceResponse topic: the partition identifier, the per-partition error
// code (if any), the base offset assigned to the first appended record and the
// log append time.
type ProduceResponsePartition struct {
	// PartitionID is the zero-based partition index within the topic.
	PartitionID int32
	// ErrorCode is 0 on success and a non-zero protocol error code otherwise.
	ErrorCode int16
	// BaseOffset is the absolute log offset assigned to the first record in
	// the appended batch.
	BaseOffset int64
	// LogAppendTime is the server-assigned append timestamp for the batch, in
	// milliseconds since the epoch. A value of -1 means the broker did not
	// assign a timestamp.
	LogAppendTime int64
}

// ProduceResponseTopic is a single topic entry within a ProduceResponse: the
// topic name and the per-partition results.
type ProduceResponseTopic struct {
	// Name is the topic name.
	Name string
	// Partitions is the list of per-partition append results.
	Partitions []ProduceResponsePartition
}

// ProduceResponse is the body of the Produce API (apiKey 0). It reports, for
// each requested topic/partition, the outcome of the append operation.
type ProduceResponse struct {
	// Topics is the list of topic/partition append results.
	Topics []ProduceResponseTopic
}

// Encode writes the ProduceResponse body to w using the primitive codec
// functions. It returns any error from the underlying writer.
func (r *ProduceResponse) Encode(w io.Writer) error {
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
			if _, err := PutInt16(w, p.ErrorCode); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.BaseOffset); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.LogAppendTime); err != nil {
				return err
			}
		}
	}
	return nil
}

// Decode reads the ProduceResponse body from r using the primitive codec
// functions. A null topics array is decoded as nil. It returns any error from
// the underlying reader.
func (r *ProduceResponse) Decode(rd io.Reader) error {
	topicCount, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if topicCount < 0 {
		r.Topics = nil
		return nil
	}
	r.Topics = make([]ProduceResponseTopic, topicCount)
	for i := 0; i < topicCount; i++ {
		var t ProduceResponseTopic
		if t.Name, err = String(rd); err != nil {
			return err
		}
		partCount, err := ArrayHeader(rd)
		if err != nil {
			return err
		}
		if partCount < 0 {
			t.Partitions = nil
			r.Topics[i] = t
			continue
		}
		t.Partitions = make([]ProduceResponsePartition, partCount)
		for j := 0; j < partCount; j++ {
			var p ProduceResponsePartition
			if p.PartitionID, err = Int32(rd); err != nil {
				return err
			}
			if p.ErrorCode, err = Int16(rd); err != nil {
				return err
			}
			if p.BaseOffset, err = Int64(rd); err != nil {
				return err
			}
			if p.LogAppendTime, err = Int64(rd); err != nil {
				return err
			}
			t.Partitions[j] = p
		}
		r.Topics[i] = t
	}
	return nil
}

// FetchResponsePartition is a single partition entry within a FetchResponse
// topic: the partition identifier, the per-partition error code (if any), the
// high watermark, the log start offset and the returned record set.
type FetchResponsePartition struct {
	// PartitionID is the zero-based partition index within the topic.
	PartitionID int32
	// ErrorCode is 0 on success and a non-zero protocol error code otherwise.
	ErrorCode int16
	// HighWatermark is the offset of the last committed (replicated) record
	// plus one; consumers may not read past this offset.
	HighWatermark int64
	// LogStartOffset is the offset of the earliest retained record in the
	// partition.
	LogStartOffset int64
	// RecordSet is the raw, length-prefixed byte sequence of records returned
	// for this partition. It may be empty when no records matched the fetch.
	RecordSet []byte
}

// FetchResponseTopic is a single topic entry within a FetchResponse: the topic
// name and the per-partition fetch results.
type FetchResponseTopic struct {
	// Name is the topic name.
	Name string
	// Partitions is the list of per-partition fetch results.
	Partitions []FetchResponsePartition
}

// FetchResponse is the body of the Fetch API (apiKey 1). It reports, for each
// requested topic/partition, the fetched record data and the partition's
// watermark positions.
type FetchResponse struct {
	// Topics is the list of topic/partition fetch results.
	Topics []FetchResponseTopic
}

// Encode writes the FetchResponse body to w using the primitive codec
// functions. It returns any error from the underlying writer.
func (r *FetchResponse) Encode(w io.Writer) error {
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
			if _, err := PutInt16(w, p.ErrorCode); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.HighWatermark); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.LogStartOffset); err != nil {
				return err
			}
			if _, err := PutBytes(w, p.RecordSet); err != nil {
				return err
			}
		}
	}
	return nil
}

// Decode reads the FetchResponse body from r using the primitive codec
// functions. A null topics array is decoded as nil. It returns any error from
// the underlying reader.
func (r *FetchResponse) Decode(rd io.Reader) error {
	topicCount, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if topicCount < 0 {
		r.Topics = nil
		return nil
	}
	r.Topics = make([]FetchResponseTopic, topicCount)
	for i := 0; i < topicCount; i++ {
		var t FetchResponseTopic
		if t.Name, err = String(rd); err != nil {
			return err
		}
		partCount, err := ArrayHeader(rd)
		if err != nil {
			return err
		}
		if partCount < 0 {
			t.Partitions = nil
			r.Topics[i] = t
			continue
		}
		t.Partitions = make([]FetchResponsePartition, partCount)
		for j := 0; j < partCount; j++ {
			var p FetchResponsePartition
			if p.PartitionID, err = Int32(rd); err != nil {
				return err
			}
			if p.ErrorCode, err = Int16(rd); err != nil {
				return err
			}
			if p.HighWatermark, err = Int64(rd); err != nil {
				return err
			}
			if p.LogStartOffset, err = Int64(rd); err != nil {
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

// ApiVersion describes a single API key and the range of versions the server
// supports for it.
type ApiVersion struct {
	// ApiKey identifies the API.
	ApiKey int16
	// MinVersion is the lowest API version the server supports for this key.
	MinVersion int16
	// MaxVersion is the highest API version the server supports for this key.
	MaxVersion int16
}

// ApiVersionsResponse is the body of the ApiVersions API (apiKey 12). It
// lists every API key the server supports along with the supported version
// range for each.
type ApiVersionsResponse struct {
	// ApiKeys is the list of supported API keys and their version ranges.
	ApiKeys []ApiVersion
}

// Encode writes the ApiVersionsResponse body to w using the primitive codec
// functions. It returns any error from the underlying writer.
func (r *ApiVersionsResponse) Encode(w io.Writer) error {
	if _, err := PutArrayHeader(w, len(r.ApiKeys)); err != nil {
		return err
	}
	for _, a := range r.ApiKeys {
		if _, err := PutInt16(w, a.ApiKey); err != nil {
			return err
		}
		if _, err := PutInt16(w, a.MinVersion); err != nil {
			return err
		}
		if _, err := PutInt16(w, a.MaxVersion); err != nil {
			return err
		}
	}
	return nil
}

// Decode reads the ApiVersionsResponse body from r using the primitive codec
// functions. A null apiKeys array is decoded as nil. It returns any error from
// the underlying reader.
func (r *ApiVersionsResponse) Decode(rd io.Reader) error {
	count, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if count < 0 {
		r.ApiKeys = nil
		return nil
	}
	r.ApiKeys = make([]ApiVersion, count)
	for i := 0; i < count; i++ {
		var a ApiVersion
		if a.ApiKey, err = Int16(rd); err != nil {
			return err
		}
		if a.MinVersion, err = Int16(rd); err != nil {
			return err
		}
		if a.MaxVersion, err = Int16(rd); err != nil {
			return err
		}
		r.ApiKeys[i] = a
	}
	return nil
}

// BrokerMetadata represents a broker node in MetadataResponse.
type BrokerMetadata struct {
	NodeID int32
	Host   string
	Port   int32
}

// PartitionMetadata represents a partition in MetadataResponse.
type PartitionMetadata struct {
	PartitionID int32
	Leader      int32
	Replicas    []int32
	ISR         []int32
}

// TopicMetadata represents a topic entry in MetadataResponse.
type TopicMetadata struct {
	Name       string
	ErrorCode  int16
	Partitions []PartitionMetadata
}

// MetadataResponse is the body of the Metadata API (apiKey 2).
type MetadataResponse struct {
	Brokers []BrokerMetadata
	Topics  []TopicMetadata
}

// Encode writes the MetadataResponse body to w.
func (r *MetadataResponse) Encode(w io.Writer) error {
	if _, err := PutArrayHeader(w, len(r.Brokers)); err != nil {
		return err
	}
	for _, b := range r.Brokers {
		if _, err := PutInt32(w, b.NodeID); err != nil {
			return err
		}
		if _, err := PutString(w, b.Host); err != nil {
			return err
		}
		if _, err := PutInt32(w, b.Port); err != nil {
			return err
		}
	}

	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutInt16(w, t.ErrorCode); err != nil {
			return err
		}
		if _, err := PutArrayHeader(w, len(t.Partitions)); err != nil {
			return err
		}
		for _, p := range t.Partitions {
			if _, err := PutInt32(w, p.PartitionID); err != nil {
				return err
			}
			if _, err := PutInt32(w, p.Leader); err != nil {
				return err
			}
			if _, err := PutArrayHeader(w, len(p.Replicas)); err != nil {
				return err
			}
			for _, rep := range p.Replicas {
				if _, err := PutInt32(w, rep); err != nil {
					return err
				}
			}
			if _, err := PutArrayHeader(w, len(p.ISR)); err != nil {
				return err
			}
			for _, isrNode := range p.ISR {
				if _, err := PutInt32(w, isrNode); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Decode reads the MetadataResponse body from rd.
func (r *MetadataResponse) Decode(rd io.Reader) error {
	brokerCount, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if brokerCount >= 0 {
		r.Brokers = make([]BrokerMetadata, brokerCount)
		for i := 0; i < brokerCount; i++ {
			var b BrokerMetadata
			if b.NodeID, err = Int32(rd); err != nil {
				return err
			}
			if b.Host, err = String(rd); err != nil {
				return err
			}
			if b.Port, err = Int32(rd); err != nil {
				return err
			}
			r.Brokers[i] = b
		}
	}

	topicCount, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if topicCount >= 0 {
		r.Topics = make([]TopicMetadata, topicCount)
		for i := 0; i < topicCount; i++ {
			var t TopicMetadata
			if t.Name, err = String(rd); err != nil {
				return err
			}
			if t.ErrorCode, err = Int16(rd); err != nil {
				return err
			}
			partCount, err := ArrayHeader(rd)
			if err != nil {
				return err
			}
			if partCount >= 0 {
				t.Partitions = make([]PartitionMetadata, partCount)
				for j := 0; j < partCount; j++ {
					var p PartitionMetadata
					if p.PartitionID, err = Int32(rd); err != nil {
						return err
					}
					if p.Leader, err = Int32(rd); err != nil {
						return err
					}
					repCount, err := ArrayHeader(rd)
					if err != nil {
						return err
					}
					if repCount >= 0 {
						p.Replicas = make([]int32, repCount)
						for k := 0; k < repCount; k++ {
							if p.Replicas[k], err = Int32(rd); err != nil {
								return err
							}
						}
					}
					isrCount, err := ArrayHeader(rd)
					if err != nil {
						return err
					}
					if isrCount >= 0 {
						p.ISR = make([]int32, isrCount)
						for k := 0; k < isrCount; k++ {
							if p.ISR[k], err = Int32(rd); err != nil {
								return err
							}
						}
					}
					t.Partitions[j] = p
				}
			}
			r.Topics[i] = t
		}
	}
	return nil
}

// CreateTopicResponseTopic represents a single topic outcome in CreateTopicsResponse.
type CreateTopicResponseTopic struct {
	Name      string
	ErrorCode int16
}

// CreateTopicsResponse is the body of the CreateTopics API (apiKey 3).
type CreateTopicsResponse struct {
	Topics []CreateTopicResponseTopic
}

// Encode writes the CreateTopicsResponse body to w.
func (r *CreateTopicsResponse) Encode(w io.Writer) error {
	if _, err := PutArrayHeader(w, len(r.Topics)); err != nil {
		return err
	}
	for _, t := range r.Topics {
		if _, err := PutString(w, t.Name); err != nil {
			return err
		}
		if _, err := PutInt16(w, t.ErrorCode); err != nil {
			return err
		}
	}
	return nil
}

// Decode reads the CreateTopicsResponse body from rd.
func (r *CreateTopicsResponse) Decode(rd io.Reader) error {
	count, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if count < 0 {
		r.Topics = nil
		return nil
	}
	r.Topics = make([]CreateTopicResponseTopic, count)
	for i := 0; i < count; i++ {
		var t CreateTopicResponseTopic
		var err error
		if t.Name, err = String(rd); err != nil {
			return err
		}
		if t.ErrorCode, err = Int16(rd); err != nil {
			return err
		}
		r.Topics[i] = t
	}
	return nil
}

// JoinGroupMember represents a member metadata in JoinGroupResponse.
type JoinGroupMember struct {
	MemberID string
	Metadata []byte
}

// JoinGroupResponse (apiKey 4)
type JoinGroupResponse struct {
	ErrorCode    int16
	GenerationID int32
	ProtocolName string
	LeaderID     string
	MemberID     string
	Members      []JoinGroupMember
}

func (r *JoinGroupResponse) Encode(w io.Writer) error {
	if _, err := PutInt16(w, r.ErrorCode); err != nil {
		return err
	}
	if _, err := PutInt32(w, r.GenerationID); err != nil {
		return err
	}
	if _, err := PutString(w, r.ProtocolName); err != nil {
		return err
	}
	if _, err := PutString(w, r.LeaderID); err != nil {
		return err
	}
	if _, err := PutString(w, r.MemberID); err != nil {
		return err
	}
	if _, err := PutArrayHeader(w, len(r.Members)); err != nil {
		return err
	}
	for _, m := range r.Members {
		if _, err := PutString(w, m.MemberID); err != nil {
			return err
		}
		if _, err := PutBytes(w, m.Metadata); err != nil {
			return err
		}
	}
	return nil
}

func (r *JoinGroupResponse) Decode(rd io.Reader) error {
	var err error
	if r.ErrorCode, err = Int16(rd); err != nil {
		return err
	}
	if r.GenerationID, err = Int32(rd); err != nil {
		return err
	}
	if r.ProtocolName, err = String(rd); err != nil {
		return err
	}
	if r.LeaderID, err = String(rd); err != nil {
		return err
	}
	if r.MemberID, err = String(rd); err != nil {
		return err
	}
	count, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if count >= 0 {
		r.Members = make([]JoinGroupMember, count)
		for i := 0; i < count; i++ {
			var m JoinGroupMember
			if m.MemberID, err = String(rd); err != nil {
				return err
			}
			if m.Metadata, err = Bytes(rd); err != nil {
				return err
			}
			r.Members[i] = m
		}
	}
	return nil
}

// SyncGroupResponse (apiKey 5)
type SyncGroupResponse struct {
	ErrorCode  int16
	Assignment []byte
}

func (r *SyncGroupResponse) Encode(w io.Writer) error {
	if _, err := PutInt16(w, r.ErrorCode); err != nil {
		return err
	}
	if _, err := PutBytes(w, r.Assignment); err != nil {
		return err
	}
	return nil
}

func (r *SyncGroupResponse) Decode(rd io.Reader) error {
	var err error
	if r.ErrorCode, err = Int16(rd); err != nil {
		return err
	}
	if r.Assignment, err = Bytes(rd); err != nil {
		return err
	}
	return nil
}

// HeartbeatResponse (apiKey 6)
type HeartbeatResponse struct {
	ErrorCode int16
}

func (r *HeartbeatResponse) Encode(w io.Writer) error {
	_, err := PutInt16(w, r.ErrorCode)
	return err
}

func (r *HeartbeatResponse) Decode(rd io.Reader) error {
	var err error
	r.ErrorCode, err = Int16(rd)
	return err
}

// LeaveGroupResponse (apiKey 7)
type LeaveGroupResponse struct {
	ErrorCode int16
}

func (r *LeaveGroupResponse) Encode(w io.Writer) error {
	_, err := PutInt16(w, r.ErrorCode)
	return err
}

func (r *LeaveGroupResponse) Decode(rd io.Reader) error {
	var err error
	r.ErrorCode, err = Int16(rd)
	return err
}

// OffsetCommitResponsePartition represents partition result in OffsetCommitResponse.
type OffsetCommitResponsePartition struct {
	PartitionID int32
	ErrorCode   int16
}

// OffsetCommitResponseTopic represents topic result in OffsetCommitResponse.
type OffsetCommitResponseTopic struct {
	Name       string
	Partitions []OffsetCommitResponsePartition
}

// OffsetCommitResponse (apiKey 8)
type OffsetCommitResponse struct {
	Topics []OffsetCommitResponseTopic
}

func (r *OffsetCommitResponse) Encode(w io.Writer) error {
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
			if _, err := PutInt16(w, p.ErrorCode); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetCommitResponse) Decode(rd io.Reader) error {
	tCount, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if tCount >= 0 {
		r.Topics = make([]OffsetCommitResponseTopic, tCount)
		for i := 0; i < tCount; i++ {
			var t OffsetCommitResponseTopic
			if t.Name, err = String(rd); err != nil {
				return err
			}
			pCount, err := ArrayHeader(rd)
			if err != nil {
				return err
			}
			if pCount >= 0 {
				t.Partitions = make([]OffsetCommitResponsePartition, pCount)
				for j := 0; j < pCount; j++ {
					var p OffsetCommitResponsePartition
					if p.PartitionID, err = Int32(rd); err != nil {
						return err
					}
					if p.ErrorCode, err = Int16(rd); err != nil {
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

// OffsetFetchResponsePartition represents partition result in OffsetFetchResponse.
type OffsetFetchResponsePartition struct {
	PartitionID int32
	Offset      int64
	ErrorCode   int16
}

// OffsetFetchResponseTopic represents topic result in OffsetFetchResponse.
type OffsetFetchResponseTopic struct {
	Name       string
	Partitions []OffsetFetchResponsePartition
}

// OffsetFetchResponse (apiKey 9)
type OffsetFetchResponse struct {
	Topics []OffsetFetchResponseTopic
}

func (r *OffsetFetchResponse) Encode(w io.Writer) error {
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
			if _, err := PutInt16(w, p.ErrorCode); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetFetchResponse) Decode(rd io.Reader) error {
	tCount, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if tCount >= 0 {
		r.Topics = make([]OffsetFetchResponseTopic, tCount)
		for i := 0; i < tCount; i++ {
			var t OffsetFetchResponseTopic
			if t.Name, err = String(rd); err != nil {
				return err
			}
			pCount, err := ArrayHeader(rd)
			if err != nil {
				return err
			}
			if pCount >= 0 {
				t.Partitions = make([]OffsetFetchResponsePartition, pCount)
				for j := 0; j < pCount; j++ {
					var p OffsetFetchResponsePartition
					if p.PartitionID, err = Int32(rd); err != nil {
						return err
					}
					if p.Offset, err = Int64(rd); err != nil {
						return err
					}
					if p.ErrorCode, err = Int16(rd); err != nil {
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

// ListOffsetsResponsePartition represents partition result in ListOffsetsResponse.
type ListOffsetsResponsePartition struct {
	PartitionID int32
	ErrorCode   int16
	Offset      int64
}

// ListOffsetsResponseTopic represents topic result in ListOffsetsResponse.
type ListOffsetsResponseTopic struct {
	Name       string
	Partitions []ListOffsetsResponsePartition
}

// ListOffsetsResponse (apiKey 10)
type ListOffsetsResponse struct {
	Topics []ListOffsetsResponseTopic
}

func (r *ListOffsetsResponse) Encode(w io.Writer) error {
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
			if _, err := PutInt16(w, p.ErrorCode); err != nil {
				return err
			}
			if _, err := PutInt64(w, p.Offset); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ListOffsetsResponse) Decode(rd io.Reader) error {
	tCount, err := ArrayHeader(rd)
	if err != nil {
		return err
	}
	if tCount >= 0 {
		r.Topics = make([]ListOffsetsResponseTopic, tCount)
		for i := 0; i < tCount; i++ {
			var t ListOffsetsResponseTopic
			if t.Name, err = String(rd); err != nil {
				return err
			}
			pCount, err := ArrayHeader(rd)
			if err != nil {
				return err
			}
			if pCount >= 0 {
				t.Partitions = make([]ListOffsetsResponsePartition, pCount)
				for j := 0; j < pCount; j++ {
					var p ListOffsetsResponsePartition
					if p.PartitionID, err = Int32(rd); err != nil {
						return err
					}
					if p.ErrorCode, err = Int16(rd); err != nil {
						return err
					}
					if p.Offset, err = Int64(rd); err != nil {
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
