package ndn

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	enc "github.com/named-data/ndnd/std/encoding"
	ndnlib "github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"
	log "github.com/sirupsen/logrus"
	"lukechampine.com/uint128"

	"github.com/nStangl/distributed-kv-store/protocol"
)

const (
	ecsPollInterval      = 500 * time.Millisecond
	ecsHeartbeatInterval = time.Second
	ecsRequestTimeout    = 4 * time.Second
)

// ECSCallbacks are invoked when the ECS delivers commands to this server.
type ECSCallbacks interface {
	OnWriteLockSet() error
	OnWriteLockLifted() error
	OnTransferRequested(targetAddr, targetServerID, rangeStart, rangeEnd string) error
	OnMetadataUpdated(protocol.Metadata) error
	OnJoinComplete() error
}

// ECSClient is the server-side NDN client for the ECS coordinator.
// It joins the ECS on startup, then polls for commands and sends heartbeats.
type ECSClient struct {
	engine    ndnlib.Engine
	serverID  string
	ndnAddr   string
	callbacks ECSCallbacks
	quit      chan struct{}
	pollSeq   atomic.Uint64
	mu        sync.Mutex
	joined    bool

	// UUID assigned by ECS on join
	id uuid.UUID
}

func NewECSClient(engine ndnlib.Engine, serverID, ndnAddr string) *ECSClient {
	return &ECSClient{
		engine:   engine,
		serverID: serverID,
		ndnAddr:  ndnAddr,
		quit:     make(chan struct{}),
	}
}

func (c *ECSClient) SetCallbacks(cb ECSCallbacks) {
	c.callbacks = cb
}

// Join sends a join Interest to the ECS coordinator.
func (c *ECSClient) Join() error {
	req := ECSJoinRequest{
		ServerID: c.serverID,
		NdnAddr:  c.ndnAddr,
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal join request: %w", err)
	}

	name, err := enc.NameFromStr(fmt.Sprintf("/ecs/join/%s", c.serverID))
	if err != nil {
		return fmt.Errorf("failed to build join name: %w", err)
	}
	name = name.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now())))

	resp, err := c.express(name, enc.Wire{payload})
	if err != nil {
		return fmt.Errorf("failed to join ECS: %w", err)
	}

	var result ECSGenericResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse join response: %w", err)
	}

	if result.Status != "ok" {
		return fmt.Errorf("ECS join failed: %s", result.Message)
	}

	nodeID, err := uuid.Parse(result.Message)
	if err != nil {
		return fmt.Errorf("failed to parse ECS-assigned uuid %q: %w", result.Message, err)
	}

	c.mu.Lock()
	c.joined = true
	c.id = nodeID
	c.mu.Unlock()

	log.Infof("joined ECS network (uuid=%s)", result.Message)
	return nil
}

// StartPolling begins the poll loop. Returns a channel of errors.
func (c *ECSClient) StartPolling() <-chan error {
	ers := make(chan error, 10)

	go func() {
		defer close(ers)
		tick := time.NewTicker(ecsPollInterval)
		defer tick.Stop()

		for {
			select {
			case <-c.quit:
				return
			case <-tick.C:
				if err := c.poll(); err != nil {
					ers <- err
				}
			}
		}
	}()

	return ers
}

// StartHeartbeats sends periodic heartbeats. Returns a channel of errors.
func (c *ECSClient) StartHeartbeats() <-chan error {
	ers := make(chan error, 10)

	go func() {
		defer close(ers)
		tick := time.NewTicker(ecsHeartbeatInterval)
		defer tick.Stop()

		for {
			select {
			case <-c.quit:
				return
			case <-tick.C:
				if err := c.sendHeartbeat(); err != nil {
					ers <- err
				}
			}
		}
	}()

	return ers
}

func (c *ECSClient) Close() {
	close(c.quit)
}

func (c *ECSClient) ID() uuid.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id
}

// Leave initiates a graceful leave from the ECS.
func (c *ECSClient) Leave() error {
	name, err := enc.NameFromStr(fmt.Sprintf("/ecs/leave/%s", c.serverID))
	if err != nil {
		return err
	}
	name = name.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now())))

	resp, err := c.express(name, nil)
	if err != nil {
		return fmt.Errorf("failed to leave ECS: %w", err)
	}

	var result ECSGenericResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse leave response: %w", err)
	}
	return nil
}

// NotifyTransferDone tells the ECS that a data transfer has completed.
func (c *ECSClient) NotifyTransferDone() error {
	name, err := enc.NameFromStr(fmt.Sprintf("/ecs/transfer-done/%s", c.serverID))
	if err != nil {
		return err
	}
	name = name.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now())))

	resp, err := c.express(name, nil)
	if err != nil {
		return fmt.Errorf("failed to notify transfer done: %w", err)
	}

	var result ECSGenericResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse transfer-done response: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

func (c *ECSClient) poll() error {
	c.mu.Lock()
	if !c.joined {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	seq := c.pollSeq.Add(1)
	name, err := enc.NameFromStr(fmt.Sprintf("/ecs/poll/%s/%d", c.serverID, seq))
	if err != nil {
		return fmt.Errorf("failed to build poll name: %w", err)
	}

	resp, err := c.express(name, nil)
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}

	var result ECSPollResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse poll response: %w", err)
	}

	if result.Command == nil {
		return nil
	}

	return c.processCommand(result.Command)
}

func (c *ECSClient) processCommand(cmd *ECSCommand) error {
	log.Infof("ECS client: received %s command", cmd.Type)

	switch cmd.Type {
	case ECSCmdMetadata:
		meta, err := ecsParseMetadata(cmd.Metadata)
		if err != nil {
			return fmt.Errorf("failed to parse metadata: %w", err)
		}
		if c.callbacks != nil {
			if err := c.callbacks.OnMetadataUpdated(meta); err != nil {
				return fmt.Errorf("metadata callback failed: %w", err)
			}
		}

	case ECSCmdSetWriteLock:
		if c.callbacks != nil {
			if err := c.callbacks.OnWriteLockSet(); err != nil {
				return fmt.Errorf("write lock callback failed: %w", err)
			}
		}
		return c.sendAck(string(ECSCmdSetWriteLock))

	case ECSCmdLiftWriteLock:
		if c.callbacks != nil {
			if err := c.callbacks.OnWriteLockLifted(); err != nil {
				return fmt.Errorf("lift write lock callback failed: %w", err)
			}
		}
		return c.sendAck(string(ECSCmdLiftWriteLock))

	case ECSCmdInvokeTransfer:
		targetAddr := cmd.Address
		var targetServerID, rangeStart, rangeEnd string
		if parts := strings.SplitN(cmd.Metadata, ",", 3); len(parts) == 3 {
			targetServerID = parts[0]
			rangeStart = parts[1]
			rangeEnd = parts[2]
		}
		if c.callbacks != nil {
			if err := c.callbacks.OnTransferRequested(targetAddr, targetServerID, rangeStart, rangeEnd); err != nil {
				log.Errorf("transfer callback failed: %v", err)
			}
		}

	case ECSCmdJoinComplete:
		if c.callbacks != nil {
			if err := c.callbacks.OnJoinComplete(); err != nil {
				return fmt.Errorf("join complete callback failed: %w", err)
			}
		}
		return c.sendAck(string(ECSCmdJoinComplete))
	}

	return nil
}

func (c *ECSClient) sendAck(cmdType string) error {
	name, err := enc.NameFromStr(fmt.Sprintf("/ecs/ack/%s/%s", c.serverID, cmdType))
	if err != nil {
		return err
	}
	name = name.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now())))

	resp, err := c.express(name, nil)
	if err != nil {
		return fmt.Errorf("failed to send ack for %s: %w", cmdType, err)
	}

	var result ECSGenericResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return fmt.Errorf("failed to parse ack response: %w", err)
	}
	return nil
}

func (c *ECSClient) sendHeartbeat() error {
	c.mu.Lock()
	if !c.joined {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	name, err := enc.NameFromStr(fmt.Sprintf("/ecs/heartbeat/%s", c.serverID))
	if err != nil {
		return err
	}
	name = name.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now())))

	_, err = c.express(name, nil)
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}
	return nil
}

func (c *ECSClient) express(name enc.Name, appParam enc.Wire) ([]byte, error) {
	intCfg := &ndnlib.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(ecsRequestTimeout),
		Nonce:       utils.ConvertNonce(c.engine.Timer().Nonce()),
	}

	interest, err := c.engine.Spec().MakeInterest(name, intCfg, appParam, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Interest: %w", err)
	}

	type result struct {
		data []byte
		err  error
	}

	ch := make(chan result, 1)

	err = c.engine.Express(interest, func(args ndnlib.ExpressCallbackArgs) {
		switch args.Result {
		case ndnlib.InterestResultData:
			ch <- result{data: args.Data.Content().Join()}
		case ndnlib.InterestResultNack:
			ch <- result{err: fmt.Errorf("NACKed (reason=%d)", args.NackReason)}
		case ndnlib.InterestResultTimeout:
			ch <- result{err: fmt.Errorf("timed out for %s", name)}
		case ndnlib.InterestCancelled:
			ch <- result{err: fmt.Errorf("cancelled for %s", name)}
		default:
			ch <- result{err: fmt.Errorf("unexpected result %d for %s", args.Result, name)}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to express Interest: %w", err)
	}

	r := <-ch
	return r.data, r.err
}

// ---------------------------------------------------------------------------
// Metadata parsing
// ---------------------------------------------------------------------------

// ecsParseMetadata parses the serialized format produced by protocol.Metadata.Bytes():
//
//	readRep,writeRep,uuid,start,end,privateAddr,addr;...
func ecsParseMetadata(raw string) (protocol.Metadata, error) {
	if raw == "" {
		return nil, nil
	}

	entries := strings.Split(strings.TrimRight(raw, ";"), ";")
	ranges := make(protocol.Metadata, 0, len(entries))

	for _, entry := range entries {
		if entry == "" {
			continue
		}

		parts := strings.SplitN(entry, ",", 8)
		if len(parts) != 8 {
			return nil, fmt.Errorf("invalid metadata entry: %q (got %d parts)", entry, len(parts))
		}

		readRep := parts[0] == "1"
		writeRep := parts[1] == "1"

		id, err := uuid.Parse(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid uuid %q: %w", parts[2], err)
		}

		start, err := uint128.FromString(parts[3])
		if err != nil {
			return nil, fmt.Errorf("invalid start %q: %w", parts[3], err)
		}

		end, err := uint128.FromString(parts[4])
		if err != nil {
			return nil, fmt.Errorf("invalid end %q: %w", parts[4], err)
		}

		ranges = append(ranges, protocol.KeyRange{
			ID:          id,
			Replica:     protocol.Replica{Read: readRep, Write: writeRep},
			Start:       start,
			End:         end,
			PrivateAddr: ecsResolveAddr(parts[5]),
			Addr:        ecsResolveAddr(parts[6]),
			NDNServerID: parts[7],
		})
	}

	return ranges, nil
}