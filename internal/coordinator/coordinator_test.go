package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
	"github.com/YusuffEren/mini-kafka/internal/server"
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
	store := NewOffsetStore("", 1)
	store.Commit("group-1", "topic-a", 0, 100)

	if got := store.Fetch("group-1", "topic-a", 0); got != 100 {
		t.Errorf("Fetch = %d, want 100", got)
	}
	if got := store.Fetch("group-1", "topic-a", 1); got != -1 {
		t.Errorf("Fetch uncommitted = %d, want -1", got)
	}
}

func TestGroupCoordinator_Join_and_Sync(t *testing.T) {
	store := NewOffsetStore("", 1)
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

func TestAssignors_uneven_distribution(t *testing.T) {
	members := []string{"consumer-2", "consumer-1"}
	topics := map[string]int32{"test-topic": 5}

	rangeAssignor := &RangeAssignor{}
	rResult := rangeAssignor.Assign(members, topics)
	if len(rResult["consumer-1"].Topics[0].Partitions) != 3 {
		t.Errorf("RangeAssignor consumer-1 partition count = %d, want 3", len(rResult["consumer-1"].Topics[0].Partitions))
	}
	if len(rResult["consumer-2"].Topics[0].Partitions) != 2 {
		t.Errorf("RangeAssignor consumer-2 partition count = %d, want 2", len(rResult["consumer-2"].Topics[0].Partitions))
	}

	rrAssignor := &RoundRobinAssignor{}
	rrResult := rrAssignor.Assign(members, topics)
	if len(rrResult["consumer-1"].Topics[0].Partitions) != 3 {
		t.Errorf("RoundRobinAssignor consumer-1 partition count = %d, want 3", len(rrResult["consumer-1"].Topics[0].Partitions))
	}
	if len(rrResult["consumer-2"].Topics[0].Partitions) != 2 {
		t.Errorf("RoundRobinAssignor consumer-2 partition count = %d, want 2", len(rrResult["consumer-2"].Topics[0].Partitions))
	}
}

func TestAssignor_empty_members(t *testing.T) {
	topics := map[string]int32{"test-topic": 3}

	rangeResult := (&RangeAssignor{}).Assign([]string{}, topics)
	if len(rangeResult) != 0 {
		t.Errorf("RangeAssignor empty members result = %d, want 0", len(rangeResult))
	}

	rrResult := (&RoundRobinAssignor{}).Assign([]string{}, topics)
	if len(rrResult) != 0 {
		t.Errorf("RoundRobinAssignor empty members result = %d, want 0", len(rrResult))
	}
}

func TestAssignor_zero_partitions(t *testing.T) {
	members := []string{"consumer-1", "consumer-2"}

	rangeResult := (&RangeAssignor{}).Assign(members, map[string]int32{"test-topic": 0})
	for _, m := range members {
		if len(rangeResult[m].Topics) != 0 {
			t.Errorf("RangeAssignor member %s has topics for zero partitions", m)
		}
	}

	rrResult := (&RoundRobinAssignor{}).Assign(members, map[string]int32{"test-topic": 0})
	for _, m := range members {
		if len(rrResult[m].Topics) != 0 {
			t.Errorf("RoundRobinAssignor member %s has topics for zero partitions", m)
		}
	}
}

func TestGetAssignor(t *testing.T) {
	a, err := GetAssignor("range")
	if err != nil || a.Name() != "range" {
		t.Errorf("GetAssignor range = %v, %v", a, err)
	}

	a, err = GetAssignor("roundrobin")
	if err != nil || a.Name() != "roundrobin" {
		t.Errorf("GetAssignor roundrobin = %v, %v", a, err)
	}

	a, err = GetAssignor("")
	if err != nil || a.Name() != "range" {
		t.Errorf("GetAssignor empty = %v, %v", a, err)
	}

	_, err = GetAssignor("sticky")
	if err == nil {
		t.Errorf("GetAssignor sticky expected error, got nil")
	}
}

func TestOffsetStore_persistence_and_recovery(t *testing.T) {
	dir := t.TempDir()
	store := NewOffsetStore(dir, 4)
	if err := store.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	store.Commit("group-1", "topic-a", 0, 42)
	store.Commit("group-1", "topic-a", 1, 99)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2 := NewOffsetStore(dir, 4)
	if err := store2.Start(); err != nil {
		t.Fatalf("Start recovery: %v", err)
	}
	defer func() { _ = store2.Close() }()

	if got := store2.Fetch("group-1", "topic-a", 0); got != 42 {
		t.Errorf("recovered offset = %d, want 42", got)
	}
	if got := store2.Fetch("group-1", "topic-a", 1); got != 99 {
		t.Errorf("recovered offset = %d, want 99", got)
	}
}

func TestOffsetStore_load_missing_file_is_noop(t *testing.T) {
	dir := t.TempDir()
	store := NewOffsetStore(dir, 4)
	if err := store.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = store.Close() }()
	if got := store.Fetch("group-1", "topic-a", 0); got != -1 {
		t.Errorf("Fetch uncommitted = %d, want -1", got)
	}
}

func TestOffsetStore_wrapper_commit_and_fetch(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	gc.CommitOffset("group-1", "topic-a", 2, 123)
	if got := gc.FetchOffset("group-1", "topic-a", 2); got != 123 {
		t.Errorf("FetchOffset = %d, want 123", got)
	}
}

func TestGroupCoordinator_session_timeout_removes_member(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	joinReq := &protocol.JoinGroupRequest{
		GroupID:          "timeout-group",
		SessionTimeoutMs: 200,
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
	memberID := joinResp.MemberID

	time.Sleep(1500 * time.Millisecond)

	gc.mu.RLock()
	g, ok := gc.groups["timeout-group"]
	gc.mu.RUnlock()
	if !ok {
		t.Fatal("group missing after timeout")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.Members) != 0 {
		t.Fatalf("expected member evicted, got %d members", len(g.Members))
	}
	if g.State != StateEmpty {
		t.Fatalf("expected group Empty, got %s", g.State)
	}
	if g.LeaderID != "" {
		t.Fatalf("expected empty leader, got %s", g.LeaderID)
	}
	_ = memberID
}

func TestGroupCoordinator_session_timeout_rebalances_remaining(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	r1 := &protocol.JoinGroupRequest{
		GroupID:          "rebalance-group",
		SessionTimeoutMs: 200,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m1")}},
	}
	r2 := &protocol.JoinGroupRequest{
		GroupID:          "rebalance-group",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m2")}},
	}

	resp1, err := gc.JoinGroup(ctx, r1, "client-1")
	if err != nil {
		t.Fatalf("JoinGroup r1 error: %v", err)
	}
	resp2, err := gc.JoinGroup(ctx, r2, "client-2")
	if err != nil {
		t.Fatalf("JoinGroup r2 error: %v", err)
	}

	initialGen := resp1.GenerationID

	time.Sleep(1500 * time.Millisecond)

	gc.mu.RLock()
	g := gc.groups["rebalance-group"]
	gc.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.Members) != 1 {
		t.Fatalf("expected 1 member after timeout, got %d", len(g.Members))
	}
	if _, ok := g.Members[resp2.MemberID]; !ok {
		t.Fatalf("expected member %s to remain", resp2.MemberID)
	}
	if g.State != StatePreparingRebalance {
		t.Fatalf("expected PreparingRebalance, got %s", g.State)
	}
	if g.GenerationID <= initialGen {
		t.Fatalf("expected generation > %d, got %d", initialGen, g.GenerationID)
	}
}

func TestHeartbeat_unknown_group(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	resp := gc.Heartbeat(&protocol.HeartbeatRequest{GroupID: "missing", GenerationID: 0, MemberID: "m"})
	if resp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("Heartbeat error code = %d, want %d", resp.ErrorCode, server.ErrUnknownMemberID)
	}
}

func TestHeartbeat_wrong_generation(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	resp := gc.Heartbeat(&protocol.HeartbeatRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID + 1,
		MemberID:     joinResp.MemberID,
	})
	if resp.ErrorCode != server.ErrIllegalGeneration {
		t.Errorf("Heartbeat error code = %d, want %d", resp.ErrorCode, server.ErrIllegalGeneration)
	}
}

func TestHeartbeat_unknown_member(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	resp := gc.Heartbeat(&protocol.HeartbeatRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID,
		MemberID:     "unknown",
	})
	if resp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("Heartbeat error code = %d, want %d", resp.ErrorCode, server.ErrUnknownMemberID)
	}
}

func TestHeartbeat_rebalance_in_progress(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	resp := gc.Heartbeat(&protocol.HeartbeatRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
	})
	if resp.ErrorCode != server.ErrRebalanceInProgress {
		t.Errorf("Heartbeat error code = %d, want %d", resp.ErrorCode, server.ErrRebalanceInProgress)
	}
}

func TestHeartbeat_successful_resets_timeout(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	assignor := &RangeAssignor{}
	assignmentMap := assignor.Assign([]string{joinResp.MemberID}, map[string]int32{"test-topic": 2})
	bytesAssign, err := EncodeAssignment(assignmentMap[joinResp.MemberID])
	if err != nil {
		t.Fatalf("EncodeAssignment error: %v", err)
	}
	_, err = gc.SyncGroup(ctx, &protocol.SyncGroupRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
		Assignments: []protocol.SyncGroupAssignment{
			{MemberID: joinResp.MemberID, Assignment: bytesAssign},
		},
	})
	if err != nil {
		t.Fatalf("SyncGroup error: %v", err)
	}

	resp := gc.Heartbeat(&protocol.HeartbeatRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
	})
	if resp.ErrorCode != server.ErrNone {
		t.Errorf("Heartbeat error code = %d, want %d", resp.ErrorCode, server.ErrNone)
	}
}

func TestLeaveGroup_unknown_group(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	resp := gc.LeaveGroup(&protocol.LeaveGroupRequest{GroupID: "missing", MemberID: "m"})
	if resp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("LeaveGroup error code = %d, want %d", resp.ErrorCode, server.ErrUnknownMemberID)
	}
}

func TestLeaveGroup_unknown_member(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	resp := gc.LeaveGroup(&protocol.LeaveGroupRequest{GroupID: "g", MemberID: "unknown"})
	if resp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("LeaveGroup error code = %d, want %d", resp.ErrorCode, server.ErrUnknownMemberID)
	}
	_ = joinResp.MemberID
}

func TestLeaveGroup_last_member_empties_group(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	leaveResp := gc.LeaveGroup(&protocol.LeaveGroupRequest{GroupID: "g", MemberID: joinResp.MemberID})
	if leaveResp.ErrorCode != server.ErrNone {
		t.Fatalf("LeaveGroup error code = %d, want %d", leaveResp.ErrorCode, server.ErrNone)
	}

	gc.mu.RLock()
	g := gc.groups["g"]
	gc.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.Members) != 0 {
		t.Fatalf("expected 0 members, got %d", len(g.Members))
	}
	if g.State != StateEmpty {
		t.Fatalf("expected Empty, got %s", g.State)
	}
	if g.LeaderID != "" {
		t.Fatalf("expected empty leader, got %s", g.LeaderID)
	}
}

func TestLeaveGroup_leader_leaves_triggers_rebalance(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	r1 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m1")}},
	}
	r2 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m2")}},
	}
	resp1, err := gc.JoinGroup(ctx, r1, "client-1")
	if err != nil {
		t.Fatalf("JoinGroup r1 error: %v", err)
	}
	resp2, err := gc.JoinGroup(ctx, r2, "client-2")
	if err != nil {
		t.Fatalf("JoinGroup r2 error: %v", err)
	}

	initialGen := resp1.GenerationID

	leaveResp := gc.LeaveGroup(&protocol.LeaveGroupRequest{GroupID: "g", MemberID: resp1.MemberID})
	if leaveResp.ErrorCode != server.ErrNone {
		t.Fatalf("LeaveGroup error code = %d, want %d", leaveResp.ErrorCode, server.ErrNone)
	}

	gc.mu.RLock()
	g := gc.groups["g"]
	gc.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(g.Members))
	}
	if _, ok := g.Members[resp2.MemberID]; !ok {
		t.Fatalf("expected member %s to remain", resp2.MemberID)
	}
	if g.State != StatePreparingRebalance {
		t.Fatalf("expected PreparingRebalance, got %s", g.State)
	}
	if g.GenerationID <= initialGen {
		t.Fatalf("expected generation > %d, got %d", initialGen, g.GenerationID)
	}
}

func TestSyncGroup_unknown_group(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	resp, err := gc.SyncGroup(ctx, &protocol.SyncGroupRequest{
		GroupID:      "missing",
		GenerationID: 0,
		MemberID:     "m",
	})
	if err != nil {
		t.Fatalf("SyncGroup error: %v", err)
	}
	if resp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("SyncGroup error code = %d, want %d", resp.ErrorCode, server.ErrUnknownMemberID)
	}
}

func TestSyncGroup_wrong_generation(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	resp, err := gc.SyncGroup(ctx, &protocol.SyncGroupRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID + 1,
		MemberID:     joinResp.MemberID,
	})
	if err != nil {
		t.Fatalf("SyncGroup error: %v", err)
	}
	if resp.ErrorCode != server.ErrIllegalGeneration {
		t.Errorf("SyncGroup error code = %d, want %d", resp.ErrorCode, server.ErrIllegalGeneration)
	}
}

func TestSyncGroup_unknown_member(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	resp, err := gc.SyncGroup(ctx, &protocol.SyncGroupRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID,
		MemberID:     "unknown",
	})
	if err != nil {
		t.Fatalf("SyncGroup error: %v", err)
	}
	if resp.ErrorCode != server.ErrUnknownMemberID {
		t.Errorf("SyncGroup error code = %d, want %d", resp.ErrorCode, server.ErrUnknownMemberID)
	}
}

func TestSyncGroup_non_leader_waits_until_context_cancel(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	r1 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m1")}},
	}
	r2 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m2")}},
	}
	resp1, err := gc.JoinGroup(ctx, r1, "client-1")
	if err != nil {
		t.Fatalf("JoinGroup r1 error: %v", err)
	}
	resp2, err := gc.JoinGroup(ctx, r2, "client-2")
	if err != nil {
		t.Fatalf("JoinGroup r2 error: %v", err)
	}
	_ = resp1

	syncCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	resp, err := gc.SyncGroup(syncCtx, &protocol.SyncGroupRequest{
		GroupID:      "g",
		GenerationID: resp2.GenerationID,
		MemberID:     resp2.MemberID,
	})
	if err != nil {
		t.Fatalf("SyncGroup error: %v", err)
	}
	if resp.ErrorCode != server.ErrRequestTimedOut {
		t.Errorf("SyncGroup error code = %d, want %d", resp.ErrorCode, server.ErrRequestTimedOut)
	}
}

func TestSyncGroup_leader_assignment_with_unknown_member_is_skipped(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	joinResp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	assignor := &RangeAssignor{}
	assignmentMap := assignor.Assign([]string{joinResp.MemberID}, map[string]int32{"test-topic": 2})
	bytesAssign, err := EncodeAssignment(assignmentMap[joinResp.MemberID])
	if err != nil {
		t.Fatalf("EncodeAssignment error: %v", err)
	}

	resp, err := gc.SyncGroup(ctx, &protocol.SyncGroupRequest{
		GroupID:      "g",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
		Assignments: []protocol.SyncGroupAssignment{
			{MemberID: joinResp.MemberID, Assignment: bytesAssign},
			{MemberID: "unknown", Assignment: []byte("bad")},
		},
	})
	if err != nil {
		t.Fatalf("SyncGroup error: %v", err)
	}
	if resp.ErrorCode != server.ErrNone {
		t.Errorf("SyncGroup error code = %d, want %d", resp.ErrorCode, server.ErrNone)
	}
}

func TestJoinGroup_empty_group_id(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	resp, err := gc.JoinGroup(ctx, &protocol.JoinGroupRequest{
		GroupID:          "",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("meta")}},
	}, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}
	if resp.ErrorCode != server.ErrInvalidGroupID {
		t.Errorf("JoinGroup error code = %d, want %d", resp.ErrorCode, server.ErrInvalidGroupID)
	}
}

func TestJoinGroup_existing_member_rejoins(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	r1 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("old")}},
	}
	resp1, err := gc.JoinGroup(ctx, r1, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}

	r2 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 5000,
		MemberID:         resp1.MemberID,
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("new")}},
	}
	resp2, err := gc.JoinGroup(ctx, r2, "client")
	if err != nil {
		t.Fatalf("JoinGroup error: %v", err)
	}
	if resp2.MemberID != resp1.MemberID {
		t.Errorf("MemberID changed: %s != %s", resp2.MemberID, resp1.MemberID)
	}
	if resp2.GenerationID != resp1.GenerationID {
		t.Errorf("GenerationID changed: %d != %d", resp2.GenerationID, resp1.GenerationID)
	}
}

func TestJoinGroup_second_member_joins(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	r1 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m1")}},
	}
	r2 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m2")}},
	}
	resp1, err := gc.JoinGroup(ctx, r1, "client-1")
	if err != nil {
		t.Fatalf("JoinGroup r1 error: %v", err)
	}
	resp2, err := gc.JoinGroup(ctx, r2, "client-2")
	if err != nil {
		t.Fatalf("JoinGroup r2 error: %v", err)
	}

	if resp2.LeaderID != resp1.MemberID {
		t.Errorf("LeaderID = %s, want %s", resp2.LeaderID, resp1.MemberID)
	}
}

func TestJoinGroup_leader_receives_members(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	defer gc.Close()

	ctx := context.Background()
	r1 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m1")}},
	}
	r2 := &protocol.JoinGroupRequest{
		GroupID:          "g",
		SessionTimeoutMs: 10000,
		MemberID:         "",
		ProtocolType:     "consumer",
		Protocols:        []protocol.JoinGroupProtocol{{Name: "range", Metadata: []byte("m2")}},
	}
	resp1, err := gc.JoinGroup(ctx, r1, "client-1")
	if err != nil {
		t.Fatalf("JoinGroup r1 error: %v", err)
	}
	resp2, err := gc.JoinGroup(ctx, r2, "client-2")
	if err != nil {
		t.Fatalf("JoinGroup r2 error: %v", err)
	}

	// Rejoin the leader to read the current member list.
	r1.MemberID = resp1.MemberID
	resp1Rejoin, err := gc.JoinGroup(ctx, r1, "client-1")
	if err != nil {
		t.Fatalf("JoinGroup rejoin error: %v", err)
	}
	if len(resp1Rejoin.Members) != 2 {
		t.Errorf("leader members = %d, want 2", len(resp1Rejoin.Members))
	}
	_ = resp2
}

func TestDecodeAssignment_empty_data(t *testing.T) {
	ma, err := DecodeAssignment([]byte{})
	if err != nil {
		t.Fatalf("DecodeAssignment error: %v", err)
	}
	if len(ma.Topics) != 0 {
		t.Errorf("topics = %d, want 0", len(ma.Topics))
	}
}

func TestDecodeAssignment_invalid_data_returns_error(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x01}
	_, err := DecodeAssignment(data)
	if err == nil {
		t.Errorf("expected error for invalid assignment data")
	}
}

func TestGroupCoordinator_Close_idempotent(t *testing.T) {
	store := NewOffsetStore("", 1)
	gc := NewGroupCoordinator(store)
	gc.Close()
	gc.Close()
}
