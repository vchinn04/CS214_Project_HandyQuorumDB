package ndn

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	enc "github.com/named-data/ndnd/std/encoding"
	ndnlib "github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	log "github.com/sirupsen/logrus"
	"lukechampine.com/uint128"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/util"
)

const (
	ecsNdnPrefix        = "/ecs"
	ecsReadReplication  = 2
	ecsWriteReplication = 2
	ecsMaxMissedPolls   = 15
	ecsJanitorInterval  = time.Second
	ecsDebugInterval    = 2 * time.Second
)

// ecsServerEntry tracks a connected KV server inside the ECS coordinator.
type ecsServerEntry struct {
	ServerID        string
	ID              uuid.UUID
	NdnAddr         string
	HashKey         uint128.Uint128
	LastSeen        time.Time
	CmdQueue        []ECSCommand
	State           string
	Missed          int
	PendingJoinFrom string // server ID of the new server whose join triggered a rebalance on this server
}

// Interest patterns:
//
//	/ecs/join/<server-id>/<timestamp>          (AppParam: ECSJoinRequest)
//	/ecs/heartbeat/<server-id>/<timestamp>
//	/ecs/poll/<server-id>/<seq>
//	/ecs/ack/<server-id>/<cmd-type>/<timestamp>
//	/ecs/leave/<server-id>/<timestamp>
//	/ecs/transfer-done/<server-id>/<timestamp>
type ECSCoordinator struct {
	mu     sync.Mutex
	engine ndnlib.Engine
	signer ndnlib.Signer
	prefix enc.Name

	sortedKeys []uint128.Uint128
	ring       map[uint128.Uint128]*ecsServerEntry
	servers    map[string]*ecsServerEntry

	done  chan struct{}
	debug bool
}

func NewECSCoordinator(engine ndnlib.Engine, signer ndnlib.Signer, debug bool) (*ECSCoordinator, error) {
	prefix, err := enc.NameFromStr(ecsNdnPrefix)
	if err != nil {
		return nil, fmt.Errorf("invalid ECS prefix: %w", err)
	}

	return &ECSCoordinator{
		engine:     engine,
		signer:     signer,
		prefix:     prefix,
		sortedKeys: make([]uint128.Uint128, 0),
		ring:       make(map[uint128.Uint128]*ecsServerEntry),
		servers:    make(map[string]*ecsServerEntry),
		done:       make(chan struct{}),
		debug:      debug,
	}, nil
}

func (e *ECSCoordinator) Start() error {
	if err := e.engine.AttachHandler(e.prefix, e.onInterest); err != nil {
		return fmt.Errorf("failed to attach ECS handler: %w", err)
	}
	if err := e.engine.RegisterRoute(e.prefix); err != nil {
		return fmt.Errorf("failed to register ECS route: %w", err)
	}

	go e.janitor()
	if e.debug {
		go e.debugLoop()
	}

	log.Infof("NDN ECS coordinator listening on prefix %s", e.prefix)
	return nil
}

func (e *ECSCoordinator) Stop() error {
	close(e.done)
	if err := e.engine.DetachHandler(e.prefix); err != nil {
		return fmt.Errorf("failed to detach ECS handler: %w", err)
	}
	if err := e.engine.UnregisterRoute(e.prefix); err != nil {
		return fmt.Errorf("failed to unregister ECS route: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Interest dispatch
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) onInterest(args ndnlib.InterestHandlerArgs) {
	name := args.Interest.Name()
	nameStr := name.String()
	parts := strings.Split(strings.TrimPrefix(nameStr, "/"), "/")

	if len(parts) < 3 || parts[0] != "ecs" {
		log.Warnf("ECS: malformed Interest: %s", nameStr)
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "invalid_name"})
		return
	}

	op := parts[1]
	serverID := parts[2]

	log.Debugf("ECS >> %s (op=%s server=%s)", nameStr, op, serverID)

	switch op {
	case "join":
		e.handleJoin(args, name, serverID)
	case "heartbeat":
		e.handleHeartbeat(args, name, serverID)
	case "poll":
		e.handlePoll(args, name, serverID)
	case "ack":
		cmdType := ""
		if len(parts) >= 4 {
			cmdType = parts[3]
		}
		e.handleAck(args, name, serverID, cmdType)
	case "leave":
		e.handleLeave(args, name, serverID)
	case "transfer-done":
		e.handleTransferDone(args, name, serverID)
	default:
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "unknown_op"})
	}
}

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) handleJoin(args ndnlib.InterestHandlerArgs, name enc.Name, serverID string) {
	appParam := args.Interest.AppParam()
	if appParam == nil {
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "missing_params"})
		return
	}

	var req ECSJoinRequest
	if err := json.Unmarshal(appParam.Join(), &req); err != nil {
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "invalid_params"})
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.servers[serverID]; exists {
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "already_joined"})
		return
	}

	entry := &ecsServerEntry{
		ServerID: serverID,
		ID:       uuid.New(),
		NdnAddr:  req.NdnAddr,
		HashKey:  util.MD5HashUint128(serverID),
		LastSeen: time.Now(),
		CmdQueue: make([]ECSCommand, 0),
		State:    "catching_up",
	}

	e.addToRing(entry)
	log.Infof("ECS: server %s joined (addr=%s, hash=%s)", serverID, req.NdnAddr, util.Uint128BigEndian(entry.HashKey))

	if len(e.servers) == 1 {
		entry.State = "available"
		meta := e.buildReplicatedMetadata()
		entry.CmdQueue = append(entry.CmdQueue,
			ECSCommand{Type: ECSCmdMetadata, Metadata: string(meta.Bytes())},
			ECSCommand{Type: ECSCmdJoinComplete},
		)
		log.Infof("ECS: first server %s is immediately available", serverID)
	} else {
		succID := e.getSuccessorID(serverID)
		succ := e.servers[succID]
		if succ != nil && succ.State == "available" {
			succ.State = "write_lock_requested"
			succ.PendingJoinFrom = serverID
			succ.CmdQueue = append(succ.CmdQueue, ECSCommand{Type: ECSCmdSetWriteLock})
			log.Infof("ECS: queued set_write_lock for successor %s (new server %s)", succID, serverID)
		} else {
			entry.State = "available"
			meta := e.buildReplicatedMetadata()
			entry.CmdQueue = append(entry.CmdQueue, ECSCommand{Type: ECSCmdJoinComplete})
			e.broadcastMetadataLocked(meta)
		}
	}

	e.replyJSON(args, name, ECSGenericResponse{Status: "ok", Message: entry.ID.String()})
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) handleHeartbeat(args ndnlib.InterestHandlerArgs, name enc.Name, serverID string) {
	e.mu.Lock()
	if srv, ok := e.servers[serverID]; ok {
		srv.Missed = 0
		srv.LastSeen = time.Now()
	}
	e.mu.Unlock()

	e.replyJSON(args, name, ECSGenericResponse{Status: "ack"})
}

// ---------------------------------------------------------------------------
// Poll — the primary command-delivery mechanism
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) handlePoll(args ndnlib.InterestHandlerArgs, name enc.Name, serverID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	srv, ok := e.servers[serverID]
	if !ok {
		e.replyJSON(args, name, ECSPollResponse{Status: "error"})
		return
	}

	srv.Missed = 0
	srv.LastSeen = time.Now()

	if len(srv.CmdQueue) > 0 {
		cmd := srv.CmdQueue[0]
		srv.CmdQueue = srv.CmdQueue[1:]
		log.Infof("ECS: delivering %s to %s (%d queued)", cmd.Type, serverID, len(srv.CmdQueue))
		e.replyJSON(args, name, ECSPollResponse{Status: "ok", Command: &cmd})
		return
	}

	e.replyJSON(args, name, ECSPollResponse{Status: "ok"})
}

// ---------------------------------------------------------------------------
// Ack — server acknowledges a received command
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) handleAck(args ndnlib.InterestHandlerArgs, name enc.Name, serverID, cmdType string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	srv, ok := e.servers[serverID]
	if !ok {
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "unknown_server"})
		return
	}

	srv.LastSeen = time.Now()
	srv.Missed = 0

	log.Infof("ECS: ack from %s for %s (state=%s)", serverID, cmdType, srv.State)

	switch ECSCmdType(cmdType) {
	case ECSCmdSetWriteLock:
		if srv.State == "write_lock_requested" {
			srv.State = "write_locked"

			newServerID := srv.PendingJoinFrom
			newServer := e.servers[newServerID]
			if newServer != nil && newServer.State == "catching_up" {
				newRange := e.findRange(newServerID)
				srv.CmdQueue = append(srv.CmdQueue, ECSCommand{
					Type:    ECSCmdInvokeTransfer,
					Address: newServer.NdnAddr,
					Metadata: fmt.Sprintf("%s,%s,%s",
						newServer.ServerID,
						util.Uint128BigEndian(newRange.Start),
						util.Uint128BigEndian(newRange.End)),
				})
				log.Infof("ECS: queued invoke_transfer for %s → %s", serverID, newServerID)
			}
		}

	case ECSCmdLiftWriteLock:
		if srv.State == "write_lock_lift_requested" {
			srv.State = "available"

			newServerID := srv.PendingJoinFrom
			newServer := e.servers[newServerID]
			if newServer != nil && newServer.State == "catching_up" {
				newServer.State = "available"
				newServer.CmdQueue = append(newServer.CmdQueue, ECSCommand{Type: ECSCmdJoinComplete})
				log.Infof("ECS: server %s join complete", newServerID)
			}
			srv.PendingJoinFrom = ""

			meta := e.buildReplicatedMetadata()
			e.broadcastMetadataLocked(meta)
		}

	case ECSCmdJoinComplete:
		// informational — no further action
	}

	e.replyJSON(args, name, ECSGenericResponse{Status: "ok"})
}

// ---------------------------------------------------------------------------
// Leave
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) handleLeave(args ndnlib.InterestHandlerArgs, name enc.Name, serverID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	srv, ok := e.servers[serverID]
	if !ok {
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "unknown_server"})
		return
	}

	srv.State = "leaving"

	if len(e.servers) <= 1 {
		e.removeFromRing(serverID)
		e.replyJSON(args, name, ECSGenericResponse{Status: "ok", Message: "removed"})
		return
	}

	succID := e.getSuccessorID(serverID)
	if succ := e.servers[succID]; succ != nil {
		myRange := e.findRange(serverID)
		srv.CmdQueue = append(srv.CmdQueue, ECSCommand{
			Type:    ECSCmdInvokeTransfer,
			Address: succ.NdnAddr,
			Metadata: fmt.Sprintf("%s,%s,%s",
				succ.ServerID,
				util.Uint128BigEndian(myRange.Start),
				util.Uint128BigEndian(myRange.End)),
		})
	}

	e.replyJSON(args, name, ECSGenericResponse{Status: "ok"})
}

// ---------------------------------------------------------------------------
// Transfer done
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) handleTransferDone(args ndnlib.InterestHandlerArgs, name enc.Name, serverID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	srv, ok := e.servers[serverID]
	if !ok {
		e.replyJSON(args, name, ECSGenericResponse{Status: "error", Message: "unknown_server"})
		return
	}

	log.Infof("ECS: transfer-done from %s (state=%s)", serverID, srv.State)

	switch srv.State {
	case "write_locked":
		srv.State = "write_lock_lift_requested"
		srv.CmdQueue = append(srv.CmdQueue, ECSCommand{Type: ECSCmdLiftWriteLock})
		log.Infof("ECS: queued lift_write_lock for %s", serverID)

	case "leaving":
		e.removeFromRing(serverID)
		if len(e.servers) > 0 {
			meta := e.buildReplicatedMetadata()
			e.broadcastMetadataLocked(meta)
		}
		log.Infof("ECS: server %s removed after transfer", serverID)
	}

	e.replyJSON(args, name, ECSGenericResponse{Status: "ok"})
}

// ---------------------------------------------------------------------------
// Consistent hash ring
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) addToRing(entry *ecsServerEntry) {
	e.servers[entry.ServerID] = entry
	e.ring[entry.HashKey] = entry
	e.sortedKeys = append(e.sortedKeys, entry.HashKey)
	sort.Slice(e.sortedKeys, func(i, j int) bool {
		return e.sortedKeys[i].Cmp(e.sortedKeys[j]) == -1
	})
}

func (e *ECSCoordinator) removeFromRing(serverID string) {
	entry, ok := e.servers[serverID]
	if !ok {
		return
	}
	delete(e.servers, serverID)
	delete(e.ring, entry.HashKey)

	for i, k := range e.sortedKeys {
		if k.Equals(entry.HashKey) {
			e.sortedKeys = append(e.sortedKeys[:i], e.sortedKeys[i+1:]...)
			break
		}
	}
}

func (e *ECSCoordinator) getSuccessorID(serverID string) string {
	entry := e.servers[serverID]
	if entry == nil || len(e.sortedKeys) < 2 {
		return serverID
	}

	idx := sort.Search(len(e.sortedKeys), func(i int) bool {
		return e.sortedKeys[i].Cmp(entry.HashKey) != -1
	})
	if idx >= len(e.sortedKeys) {
		idx = 0
	}

	nextIdx := (idx + 1) % len(e.sortedKeys)
	return e.ring[e.sortedKeys[nextIdx]].ServerID
}

func (e *ECSCoordinator) getPredecessorID(serverID string) string {
	entry := e.servers[serverID]
	if entry == nil || len(e.sortedKeys) < 2 {
		return serverID
	}

	idx := sort.Search(len(e.sortedKeys), func(i int) bool {
		return e.sortedKeys[i].Cmp(entry.HashKey) != -1
	})
	if idx >= len(e.sortedKeys) {
		idx = 0
	}

	prevIdx := util.Modulo(idx-1, len(e.sortedKeys))
	return e.ring[e.sortedKeys[prevIdx]].ServerID
}

func (e *ECSCoordinator) findRange(serverID string) protocol.KeyRange {
	entry := e.servers[serverID]
	if entry == nil {
		return protocol.KeyRange{}
	}

	idx := sort.Search(len(e.sortedKeys), func(i int) bool {
		return e.sortedKeys[i].Cmp(entry.HashKey) != -1
	})
	if idx >= len(e.sortedKeys) {
		idx = 0
	}

	predIdx := util.Modulo(idx-1, len(e.sortedKeys))
	predKey := e.sortedKeys[predIdx]

	addr := ecsResolveAddr(entry.NdnAddr)
	return protocol.KeyRange{
		ID:          entry.ID,
		Addr:        addr,
		PrivateAddr: addr,
		Start:       predKey,
		End:         entry.HashKey,
	}
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) buildMetadata() protocol.Metadata {
	meta := make(protocol.Metadata, 0, len(e.servers))
	for _, entry := range e.servers {
		meta = append(meta, e.findRange(entry.ServerID))
	}
	return meta
}

func (e *ECSCoordinator) buildReplicatedMetadata() protocol.Metadata {
	return e.buildMetadata().MakeReplicatedMetadata(ecsReadReplication, ecsWriteReplication)
}

func (e *ECSCoordinator) broadcastMetadataLocked(meta protocol.Metadata) {
	serialized := string(meta.Bytes())
	n := 0
	for _, srv := range e.servers {
		switch srv.State {
		case "available", "write_locked", "write_lock_requested", "write_lock_lift_requested", "catching_up":
			srv.CmdQueue = append(srv.CmdQueue, ECSCommand{Type: ECSCmdMetadata, Metadata: serialized})
			n++
		}
	}
	log.Infof("ECS: broadcast metadata to %d servers", n)
}

// ---------------------------------------------------------------------------
// Janitor — detects dead servers
// ---------------------------------------------------------------------------

func (e *ECSCoordinator) janitor() {
	tick := time.NewTicker(ecsJanitorInterval)
	defer tick.Stop()

	for {
		select {
		case <-e.done:
			return
		case <-tick.C:
			e.removeDeadServers()
		}
	}
}

func (e *ECSCoordinator) removeDeadServers() {
	e.mu.Lock()
	defer e.mu.Unlock()

	var toRemove []string
	for id, srv := range e.servers {
		srv.Missed++
		if srv.Missed >= ecsMaxMissedPolls {
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		log.Infof("ECS: removing dead server %s (missed %d polls)", id, ecsMaxMissedPolls)
		e.removeFromRing(id)
	}

	if len(toRemove) > 0 && len(e.servers) > 0 {
		meta := e.buildReplicatedMetadata()
		e.broadcastMetadataLocked(meta)
	}
}

func (e *ECSCoordinator) debugLoop() {
	tick := time.NewTicker(ecsDebugInterval)
	defer tick.Stop()

	for {
		select {
		case <-e.done:
			return
		case <-tick.C:
			e.mu.Lock()
			for id, srv := range e.servers {
				log.Infof("ECS debug: %s state=%s queue=%d missed=%d", id, srv.State, len(srv.CmdQueue), srv.Missed)
			}
			e.mu.Unlock()
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ecsResolveAddr(addr string) *net.TCPAddr {
	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return &net.TCPAddr{IP: net.IPv4zero, Port: 0}
	}
	return resolved
}

func (e *ECSCoordinator) replyJSON(args ndnlib.InterestHandlerArgs, name enc.Name, resp any) {
	payload, err := json.Marshal(resp)
	if err != nil {
		log.Errorf("ECS: failed to marshal response: %v", err)
		return
	}

	dataPkt, err := e.engine.Spec().MakeData(
		name,
		&ndnlib.DataConfig{
			ContentType: optional.Some(ndnlib.ContentTypeBlob),
			Freshness:   optional.Some(1 * time.Millisecond),
		},
		enc.Wire{payload},
		e.signer,
	)
	if err != nil {
		log.Errorf("ECS: failed to encode Data: %v", err)
		return
	}

	if err := args.Reply(dataPkt.Wire); err != nil {
		log.Errorf("ECS: failed to reply: %v", err)
	}
}
