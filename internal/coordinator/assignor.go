package coordinator

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
)

// PartitionAssignment represents a single topic's assigned partition IDs.
type PartitionAssignment struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// MemberAssignment represents the complete partition assignment for a consumer group member.
type MemberAssignment struct {
	Topics []PartitionAssignment `json:"topics"`
}

// EncodeAssignment serializes MemberAssignment into binary bytes for SyncGroup payload.
func EncodeAssignment(ma MemberAssignment) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := protocol.PutArrayHeader(&buf, len(ma.Topics)); err != nil {
		return nil, err
	}
	for _, t := range ma.Topics {
		if _, err := protocol.PutString(&buf, t.Topic); err != nil {
			return nil, err
		}
		if _, err := protocol.PutArrayHeader(&buf, len(t.Partitions)); err != nil {
			return nil, err
		}
		for _, pID := range t.Partitions {
			if _, err := protocol.PutInt32(&buf, pID); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

// DecodeAssignment deserializes MemberAssignment from binary bytes.
func DecodeAssignment(data []byte) (MemberAssignment, error) {
	if len(data) == 0 {
		return MemberAssignment{}, nil
	}
	rd := bytes.NewReader(data)
	tCount, err := protocol.ArrayHeader(rd)
	if err != nil {
		return MemberAssignment{}, err
	}
	if tCount < 0 {
		return MemberAssignment{}, nil
	}

	topics := make([]PartitionAssignment, tCount)
	for i := 0; i < tCount; i++ {
		tName, err := protocol.String(rd)
		if err != nil {
			return MemberAssignment{}, err
		}
		pCount, err := protocol.ArrayHeader(rd)
		if err != nil {
			return MemberAssignment{}, err
		}
		partitions := make([]int32, pCount)
		for j := 0; j < pCount; j++ {
			pID, err := protocol.Int32(rd)
			if err != nil {
				return MemberAssignment{}, err
			}
			partitions[j] = pID
		}
		topics[i] = PartitionAssignment{Topic: tName, Partitions: partitions}
	}

	return MemberAssignment{Topics: topics}, nil
}

// Assignor defines partition assignment strategy algorithms.
type Assignor interface {
	Name() string
	Assign(members []string, topicPartitions map[string]int32) map[string]MemberAssignment
}

// RangeAssignor assigns partitions on a per-topic basis using contiguous range slicing.
type RangeAssignor struct{}

func (r *RangeAssignor) Name() string {
	return "range"
}

func (r *RangeAssignor) Assign(members []string, topicPartitions map[string]int32) map[string]MemberAssignment {
	result := make(map[string]MemberAssignment)
	if len(members) == 0 {
		return result
	}

	sortedMembers := append([]string(nil), members...)
	sort.Strings(sortedMembers)
	numMembers := len(sortedMembers)

	memberAssignmentsMap := make(map[string]map[string][]int32)
	for _, m := range sortedMembers {
		memberAssignmentsMap[m] = make(map[string][]int32)
	}

	sortedTopics := make([]string, 0, len(topicPartitions))
	for t := range topicPartitions {
		sortedTopics = append(sortedTopics, t)
	}
	sort.Strings(sortedTopics)

	for _, topic := range sortedTopics {
		numPartitions := int(topicPartitions[topic])
		if numPartitions <= 0 {
			continue
		}

		numPartitionsPerConsumer := numPartitions / numMembers
		consumersWithExtraPartition := numPartitions % numMembers

		for i, member := range sortedMembers {
			start := numPartitionsPerConsumer*i + min(i, consumersWithExtraPartition)
			length := numPartitionsPerConsumer
			if i < consumersWithExtraPartition {
				length++
			}
			for p := start; p < start+length; p++ {
				memberAssignmentsMap[member][topic] = append(memberAssignmentsMap[member][topic], int32(p))
			}
		}
	}

	for _, m := range sortedMembers {
		var paList []PartitionAssignment
		for _, topic := range sortedTopics {
			if parts, ok := memberAssignmentsMap[m][topic]; ok && len(parts) > 0 {
				paList = append(paList, PartitionAssignment{Topic: topic, Partitions: parts})
			}
		}
		result[m] = MemberAssignment{Topics: paList}
	}

	return result
}

// RoundRobinAssignor assigns all topic-partitions sequentially across sorted members.
type RoundRobinAssignor struct{}

func (rr *RoundRobinAssignor) Name() string {
	return "roundrobin"
}

type topicPartition struct {
	topic       string
	partitionID int32
}

func (rr *RoundRobinAssignor) Assign(members []string, topicPartitions map[string]int32) map[string]MemberAssignment {
	result := make(map[string]MemberAssignment)
	if len(members) == 0 {
		return result
	}

	sortedMembers := append([]string(nil), members...)
	sort.Strings(sortedMembers)
	numMembers := len(sortedMembers)

	var allTPs []topicPartition
	sortedTopics := make([]string, 0, len(topicPartitions))
	for t := range topicPartitions {
		sortedTopics = append(sortedTopics, t)
	}
	sort.Strings(sortedTopics)

	for _, t := range sortedTopics {
		for p := int32(0); p < topicPartitions[t]; p++ {
			allTPs = append(allTPs, topicPartition{topic: t, partitionID: p})
		}
	}

	memberAssignmentsMap := make(map[string]map[string][]int32)
	for _, m := range sortedMembers {
		memberAssignmentsMap[m] = make(map[string][]int32)
	}

	for i, tp := range allTPs {
		targetMember := sortedMembers[i%numMembers]
		memberAssignmentsMap[targetMember][tp.topic] = append(memberAssignmentsMap[targetMember][tp.topic], tp.partitionID)
	}

	for _, m := range sortedMembers {
		var paList []PartitionAssignment
		for _, topic := range sortedTopics {
			if parts, ok := memberAssignmentsMap[m][topic]; ok && len(parts) > 0 {
				paList = append(paList, PartitionAssignment{Topic: topic, Partitions: parts})
			}
		}
		result[m] = MemberAssignment{Topics: paList}
	}

	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func GetAssignor(name string) (Assignor, error) {
	switch name {
	case "roundrobin":
		return &RoundRobinAssignor{}, nil
	case "range", "":
		return &RangeAssignor{}, nil
	default:
		return nil, fmt.Errorf("unknown assignor strategy: %s", name)
	}
}
