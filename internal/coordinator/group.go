package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/yusuf/mini-kafka/internal/protocol"
	"github.com/yusuf/mini-kafka/internal/server"
)

type GroupState string

const (
	StateEmpty               GroupState = "Empty"
	StatePreparingRebalance  GroupState = "PreparingRebalance"
	StateCompletingRebalance GroupState = "CompletingRebalance"
	StateStable              GroupState = "Stable"
)

type Member struct {
	ID               string
	SessionTimeoutMs int32
	LastHeartbeat    time.Time
	Metadata         []byte
	Assignment       []byte
	joinRespCh       chan *protocol.JoinGroupResponse
	syncRespCh       chan *protocol.SyncGroupResponse
}

type Group struct {
	ID           string
	State        GroupState
	GenerationID int32
	LeaderID     string
	ProtocolType string
	ProtocolName string
	Members      map[string]*Member

	rebalanceTimer *time.Timer
	mu             sync.Mutex
}

func generateMemberID(clientID string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s", clientID, hex.EncodeToString(b))
}

type GroupCoordinator struct {
	mu          sync.RWMutex
	groups      map[string]*Group
	offsetStore *OffsetStore
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

func NewGroupCoordinator(offsetStore *OffsetStore) *GroupCoordinator {
	gc := &GroupCoordinator{
		groups:      make(map[string]*Group),
		offsetStore: offsetStore,
		closeCh:     make(chan struct{}),
	}

	gc.wg.Add(1)
	go gc.sessionTimeoutLoop()

	return gc
}

func (gc *GroupCoordinator) Close() {
	select {
	case <-gc.closeCh:
		return
	default:
		close(gc.closeCh)
	}
	gc.wg.Wait()
}

func (gc *GroupCoordinator) sessionTimeoutLoop() {
	defer gc.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gc.checkSessionTimeouts()
		case <-gc.closeCh:
			return
		}
	}
}

func (gc *GroupCoordinator) checkSessionTimeouts() {
	gc.mu.Lock()
	groups := make([]*Group, 0, len(gc.groups))
	for _, g := range gc.groups {
		groups = append(groups, g)
	}
	gc.mu.Unlock()

	now := time.Now()
	for _, g := range groups {
		g.mu.Lock()
		var expired []string
		for mID, m := range g.Members {
			timeout := time.Duration(m.SessionTimeoutMs) * time.Millisecond
			if timeout <= 0 {
				timeout = 45 * time.Second
			}
			if now.Sub(m.LastHeartbeat) > timeout {
				expired = append(expired, mID)
			}
		}

		if len(expired) > 0 {
			for _, mID := range expired {
				delete(g.Members, mID)
			}
			if len(g.Members) == 0 {
				g.State = StateEmpty
				g.LeaderID = ""
			} else {
				g.transitionToRebalanceLocked()
			}
		}
		g.mu.Unlock()
	}
}

func (g *Group) transitionToRebalanceLocked() {
	g.State = StatePreparingRebalance
	g.GenerationID++
	g.LeaderID = ""
	g.AssignmentCleared()

	// Notify existing waiting channels with RebalanceInProgress error if rebalancing
	for mID := range g.Members {
		if g.LeaderID == "" {
			g.LeaderID = mID
		}
	}
}

func (g *Group) AssignmentCleared() {
	for _, m := range g.Members {
		m.Assignment = nil
	}
}

// JoinGroup processes a JoinGroup request.
func (gc *GroupCoordinator) JoinGroup(ctx context.Context, req *protocol.JoinGroupRequest, clientID string) (*protocol.JoinGroupResponse, error) {
	if req.GroupID == "" {
		return &protocol.JoinGroupResponse{ErrorCode: server.ErrInvalidGroupID}, nil
	}

	gc.mu.Lock()
	g, exists := gc.groups[req.GroupID]
	if !exists {
		g = &Group{
			ID:           req.GroupID,
			State:        StateEmpty,
			ProtocolType: req.ProtocolType,
			Members:      make(map[string]*Member),
		}
		gc.groups[req.GroupID] = g
	}
	gc.mu.Unlock()

	g.mu.Lock()

	memberID := req.MemberID
	if memberID == "" {
		memberID = generateMemberID(clientID)
	}

	meta := []byte{}
	protoName := ""
	if len(req.Protocols) > 0 {
		protoName = req.Protocols[0].Name
		meta = req.Protocols[0].Metadata
	}
	g.ProtocolName = protoName

	m, mExists := g.Members[memberID]
	if !mExists {
		m = &Member{
			ID:               memberID,
			SessionTimeoutMs: req.SessionTimeoutMs,
			LastHeartbeat:    time.Now(),
			Metadata:         meta,
			joinRespCh:       make(chan *protocol.JoinGroupResponse, 1),
			syncRespCh:       make(chan *protocol.SyncGroupResponse, 1),
		}
		g.Members[memberID] = m
	} else {
		m.SessionTimeoutMs = req.SessionTimeoutMs
		m.LastHeartbeat = time.Now()
		m.Metadata = meta
		m.joinRespCh = make(chan *protocol.JoinGroupResponse, 1)
		m.syncRespCh = make(chan *protocol.SyncGroupResponse, 1)
	}

	if g.State == StateStable || g.State == StateEmpty {
		g.transitionToRebalanceLocked()
	}

	if g.LeaderID == "" {
		g.LeaderID = memberID
	}

	joinResp := &protocol.JoinGroupResponse{
		ErrorCode:    server.ErrNone,
		GenerationID: g.GenerationID,
		ProtocolName: g.ProtocolName,
		LeaderID:     g.LeaderID,
		MemberID:     memberID,
	}

	if memberID == g.LeaderID {
		var membersMeta []protocol.JoinGroupMember
		for memberK, memV := range g.Members {
			membersMeta = append(membersMeta, protocol.JoinGroupMember{
				MemberID: memberK,
				Metadata: memV.Metadata,
			})
		}
		joinResp.Members = membersMeta
	}
	g.mu.Unlock()

	return joinResp, nil
}

// SyncGroup processes a SyncGroup request.
func (gc *GroupCoordinator) SyncGroup(ctx context.Context, req *protocol.SyncGroupRequest) (*protocol.SyncGroupResponse, error) {
	gc.mu.RLock()
	g, exists := gc.groups[req.GroupID]
	gc.mu.RUnlock()

	if !exists {
		return &protocol.SyncGroupResponse{ErrorCode: server.ErrUnknownMemberID}, nil
	}

	g.mu.Lock()
	if req.GenerationID != g.GenerationID {
		g.mu.Unlock()
		return &protocol.SyncGroupResponse{ErrorCode: server.ErrIllegalGeneration}, nil
	}

	m, mExists := g.Members[req.MemberID]
	if !mExists {
		g.mu.Unlock()
		return &protocol.SyncGroupResponse{ErrorCode: server.ErrUnknownMemberID}, nil
	}

	m.LastHeartbeat = time.Now()

	// If leader sends assignments
	if req.MemberID == g.LeaderID && len(req.Assignments) > 0 {
		for _, assign := range req.Assignments {
			if targetMem, ok := g.Members[assign.MemberID]; ok {
				targetMem.Assignment = assign.Assignment
			}
		}
		g.State = StateStable

		// Notify all waiting members
		for _, mem := range g.Members {
			resp := &protocol.SyncGroupResponse{
				ErrorCode:  server.ErrNone,
				Assignment: mem.Assignment,
			}
			select {
			case mem.syncRespCh <- resp:
			default:
			}
		}
	}

	syncCh := m.syncRespCh
	g.mu.Unlock()

	select {
	case resp := <-syncCh:
		return resp, nil
	case <-time.After(30 * time.Second):
		return &protocol.SyncGroupResponse{ErrorCode: server.ErrRequestTimedOut}, nil
	case <-ctx.Done():
		return &protocol.SyncGroupResponse{ErrorCode: server.ErrRequestTimedOut}, nil
	}
}

// Heartbeat processes a Heartbeat request.
func (gc *GroupCoordinator) Heartbeat(req *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	gc.mu.RLock()
	g, exists := gc.groups[req.GroupID]
	gc.mu.RUnlock()

	if !exists {
		return &protocol.HeartbeatResponse{ErrorCode: server.ErrUnknownMemberID}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if req.GenerationID != g.GenerationID {
		return &protocol.HeartbeatResponse{ErrorCode: server.ErrIllegalGeneration}
	}

	m, exists := g.Members[req.MemberID]
	if !exists {
		return &protocol.HeartbeatResponse{ErrorCode: server.ErrUnknownMemberID}
	}

	if g.State == StatePreparingRebalance {
		return &protocol.HeartbeatResponse{ErrorCode: server.ErrRebalanceInProgress}
	}

	m.LastHeartbeat = time.Now()
	return &protocol.HeartbeatResponse{ErrorCode: server.ErrNone}
}

// LeaveGroup processes a LeaveGroup request.
func (gc *GroupCoordinator) LeaveGroup(req *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	gc.mu.RLock()
	g, exists := gc.groups[req.GroupID]
	gc.mu.RUnlock()

	if !exists {
		return &protocol.LeaveGroupResponse{ErrorCode: server.ErrUnknownMemberID}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.Members[req.MemberID]; !exists {
		return &protocol.LeaveGroupResponse{ErrorCode: server.ErrUnknownMemberID}
	}

	delete(g.Members, req.MemberID)
	if len(g.Members) == 0 {
		g.State = StateEmpty
		g.LeaderID = ""
	} else {
		g.transitionToRebalanceLocked()
	}

	return &protocol.LeaveGroupResponse{ErrorCode: server.ErrNone}
}

// CommitOffset stores offset in OffsetStore.
func (gc *GroupCoordinator) CommitOffset(groupID string, topic string, partition int32, offset int64) {
	gc.offsetStore.Commit(groupID, topic, partition, offset)
}

// FetchOffset retrieves offset from OffsetStore.
func (gc *GroupCoordinator) FetchOffset(groupID string, topic string, partition int32) int64 {
	return gc.offsetStore.Fetch(groupID, topic, partition)
}
