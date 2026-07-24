package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/yusuf/mini-kafka/internal/protocol"
)

func TestAssignors(t *testing.T) {
	members := []string{"consumer-1", "consumer-2"}
	topics := map[string]int32{"test-topic": 4}

	rangeAssignor := &RangeAssignor{}
	rResult := rangeAssignor.Assign(members, topics)
	if len(rResult["consumer-1"].Topics[0].Partitions) != 2 || len(rResult["consumer-2"].Topics[0].Partitions) != 2 {
		t.Errorf("RangeAssignor partition count mismatch: %v", rResult)
	}

	rrAssignor := &RoundRobinAssignor{}
	rrResult := rrAssignor.Assign(members, topics)
	if len(rrResult["consumer-1"].Topics[0].Partitions) != 2 || len(rrResult["consumer-2"].Topics[0].Partitions) != 2 {
		t.Errorf("RoundRobinAssignor partition count mismatch: %v", rrResult)
	}
}

func TestOffsetStore(t *testing.T) {
	store := NewOffsetStore()
	store.Commit("group-1", "topic-a", 0, 100)

	if got := store.Fetch("group-1", "topic-a", 0); got != 100 {
		t.Errorf("Fetch = %d, want 100", got)
	}
	if got := store.Fetch("group-1", "topic-a", 1); got != -1 {
		t.Errorf("Fetch uncommitted = %d, want -1", got)
	}
}

func TestGroupCoordinator_Join_and_Sync(t *testing.T) {
	store := NewOffsetStore()
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	joinReq := &protocol.JoinGroupRequest{
		GroupID:          "test-group",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols: []protocol.JoinGroupProtocol{
			{Name: "range", Metadata: []byte("meta")},
		},
	}

	joinResp, err := gc.JoinGroup(ctx, joinReq, "client-1")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup ErrorCode = %d, want 0", joinResp.ErrorCode)
	}
	if joinResp.MemberID == "" {
		t.Fatal("JoinGroup returned empty MemberID")
	}

	// Leader syncs assignment
	assignor := &RangeAssignor{}
	assignmentMap := assignor.Assign([]string{joinResp.MemberID}, map[string]int32{"test-topic": 2})
	bytesAssign, err := EncodeAssignment(assignmentMap[joinResp.MemberID])
	if err != nil {
		t.Fatalf("EncodeAssignment error: %v", err)
	}

	syncReq := &protocol.SyncGroupRequest{
		GroupID:      "test-group",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
		Assignments: []protocol.SyncGroupAssignment{
			{MemberID: joinResp.MemberID, Assignment: bytesAssign},
		},
	}

	syncResp, err := gc.SyncGroup(ctx, syncReq)
	if err != nil {
		t.Fatalf("SyncGroup error: %v", err)
	}
	if syncResp.ErrorCode != 0 {
		t.Fatalf("SyncGroup ErrorCode = %d, want 0", syncResp.ErrorCode)
	}

	decodedAssign, err := DecodeAssignment(syncResp.Assignment)
	if err != nil {
		t.Fatalf("DecodeAssignment error: %v", err)
	}
	if len(decodedAssign.Topics) != 1 || len(decodedAssign.Topics[0].Partitions) != 2 {
		t.Fatalf("unexpected decoded assignment: %v", decodedAssign)
	}
}
