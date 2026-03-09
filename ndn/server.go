package ndn

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	ndnlib "github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"
	log "github.com/sirupsen/logrus"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/server/data"
	"github.com/nStangl/distributed-kv-store/server/store"
	"github.com/nStangl/distributed-kv-store/util"
)

// Response is the JSON payload returned in NDN Data packets.
type Response struct {
	Status string `json:"status"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
}

const (
	StatusSuccess        = "success"
	StatusUpdate         = "update"
	StatusError          = "error"
	StatusNotResponsible = "not_responsible"
	StatusWriteLocked    = "write_locked"
	StatusNotFound       = "not_found"
)

// Server is an NDN-based KV store server (producer).
// It registers a prefix and handles KV Interests.
type Server struct {
	mu        sync.Mutex
	engine    ndnlib.Engine
	signer    ndnlib.Signer
	store     store.Store
	serverID  string
	prefix    enc.Name
	metadata  protocol.Metadata
	writeLock atomic.Bool
}

// NewServer creates a new NDN KV server.
func NewServer(engine ndnlib.Engine, signer ndnlib.Signer, s store.Store, serverID string) (*Server, error) {
	prefix, err := ServerPrefix(serverID)
	if err != nil {
		return nil, fmt.Errorf("invalid server prefix: %w", err)
	}

	return &Server{
		engine:   engine,
		signer:   signer,
		store:    s,
		serverID: serverID,
		prefix:   prefix,
	}, nil
}

// Start registers the NDN prefix and begins handling Interests.
func (s *Server) Start() error {
	if err := s.engine.AttachHandler(s.prefix, s.onInterest); err != nil {
		return fmt.Errorf("failed to attach NDN handler: %w", err)
	}
	if err := s.engine.RegisterRoute(s.prefix); err != nil {
		return fmt.Errorf("failed to register NDN route: %w", err)
	}

	log.Infof("NDN KV server listening on prefix %s", s.prefix)
	return nil
}

// Stop removes the NDN prefix registration.
func (s *Server) Stop() error {
	if err := s.engine.DetachHandler(s.prefix); err != nil {
		return fmt.Errorf("failed to detach NDN handler: %w", err)
	}
	if err := s.engine.UnregisterRoute(s.prefix); err != nil {
		return fmt.Errorf("failed to unregister NDN route: %w", err)
	}
	return nil
}

// SetMetadata updates the keyrange metadata.
func (s *Server) SetMetadata(metadata protocol.Metadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata = metadata
}

// SetWriteLock enables or disables the write lock.
func (s *Server) SetWriteLock(locked bool) {
	s.writeLock.Store(locked)
}

func (s *Server) onInterest(args ndnlib.InterestHandlerArgs) {
	interest := args.Interest
	name := interest.Name()

	log.Debugf(">> Interest: %s", name)

	parsed, err := ParseInterestName(name)
	if err != nil {
		log.Warnf("malformed Interest name: %s", name)
		s.replyJSON(args, name, Response{Status: StatusError, Error: "invalid_name"})
		return
	}

	switch parsed.Op {
	case OpGet:
		s.handleGet(args, name, parsed.Key)
	case OpPut:
		s.handlePut(args, name, parsed.Key, interest.AppParam())
	case OpDelete:
		s.handleDelete(args, name, parsed.Key)
	case OpKeyrange:
		s.handleKeyrange(args, name)
	case OpKeyrangeRead:
		s.handleKeyrangeRead(args, name)
	case OpSvTransfer:
		s.handleSvTransfer(args, name, parsed.Key, interest.AppParam())
	case OpSvReplicate:
		s.handleSvReplicate(args, name, parsed.Key, interest.AppParam())
	default:
		s.replyJSON(args, name, Response{Status: StatusError, Error: "unknown_operation"})
	}
}

func (s *Server) handleGet(args ndnlib.InterestHandlerArgs, name enc.Name, key string) {
	if key == "" {
		s.replyJSON(args, name, Response{Status: StatusError, Error: "missing_key"})
		return
	}

	if !s.responsibleFor(key, false) {
		s.replyJSON(args, name, Response{Status: StatusNotResponsible})
		return
	}

	result, err := s.store.Get(key)
	if err != nil {
		log.Errorf("store.Get failed: %v", err)
		s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
		return
	}

	if result.Kind == data.Present {
		s.replyJSON(args, name, Response{Status: StatusSuccess, Key: key, Value: result.Value})
	} else {
		s.replyJSON(args, name, Response{Status: StatusNotFound, Key: key})
	}
}

func (s *Server) handlePut(args ndnlib.InterestHandlerArgs, name enc.Name, key string, appParam enc.Wire) {
	if key == "" {
		s.replyJSON(args, name, Response{Status: StatusError, Error: "missing_key"})
		return
	}

	if s.writeLock.Load() {
		s.replyJSON(args, name, Response{Status: StatusWriteLocked})
		return
	}

	if !s.responsibleFor(key, true) {
		s.replyJSON(args, name, Response{Status: StatusNotResponsible})
		return
	}

	value := string(appParam.Join())
	if value == "" {
		s.replyJSON(args, name, Response{Status: StatusError, Error: "missing_value"})
		return
	}

	existing, err := s.store.Get(key)
	if err != nil {
		log.Errorf("store.Get failed: %v", err)
		s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
		return
	}

	if err := s.store.Set(key, value); err != nil {
		log.Errorf("store.Set failed: %v", err)
		s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
		return
	}

	status := StatusSuccess
	if existing.Kind == data.Present {
		status = StatusUpdate
	}
	s.replyJSON(args, name, Response{Status: status, Key: key})
}

func (s *Server) handleDelete(args ndnlib.InterestHandlerArgs, name enc.Name, key string) {
	if key == "" {
		s.replyJSON(args, name, Response{Status: StatusError, Error: "missing_key"})
		return
	}

	if s.writeLock.Load() {
		s.replyJSON(args, name, Response{Status: StatusWriteLocked})
		return
	}

	if !s.responsibleFor(key, true) {
		s.replyJSON(args, name, Response{Status: StatusNotResponsible})
		return
	}

	result, err := s.store.Get(key)
	if err != nil {
		log.Errorf("store.Get failed: %v", err)
		s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
		return
	}

	if result.Kind != data.Present {
		s.replyJSON(args, name, Response{Status: StatusNotFound, Key: key})
		return
	}

	if err := s.store.Del(key); err != nil {
		log.Errorf("store.Del failed: %v", err)
		s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
		return
	}

	s.replyJSON(args, name, Response{Status: StatusSuccess, Key: key, Value: result.Value})
}

func (s *Server) handleKeyrange(args ndnlib.InterestHandlerArgs, name enc.Name) {
	s.mu.Lock()
	meta := s.metadata
	s.mu.Unlock()

	serialized := meta.FilterReplicas().HexString()
	s.replyJSON(args, name, Response{Status: StatusSuccess, Value: serialized})
}

func (s *Server) handleKeyrangeRead(args ndnlib.InterestHandlerArgs, name enc.Name) {
	s.mu.Lock()
	meta := s.metadata
	s.mu.Unlock()

	readableRanges, err := meta.ReadableFrom(2) // replicationFactor = 2
	if err != nil {
		log.Errorf("failed to compute readable ranges: %v", err)
		s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
		return
	}

	s.replyJSON(args, name, Response{Status: StatusSuccess, Value: readableRanges.HexString()})
}

// Engine returns the underlying NDN engine (used for server-to-server operations).
func (s *Server) Engine() ndnlib.Engine {
	return s.engine
}

// SvTransfer sends a transfer Interest to targetServerID, writing key/value on
// the remote server. An empty value signals a deletion.
// This is called by the source server during a rebalancing handoff.
func (s *Server) SvTransfer(targetServerID, key, value string) (*Response, error) {
	name, err := SvTransferName(targetServerID, key)
	if err != nil {
		return nil, fmt.Errorf("failed to build SV-TRANSFER name: %w", err)
	}
	var payload enc.Wire
	if value != "" {
		payload = enc.Wire{[]byte(value)}
	}
	return s.express(name, payload)
}

// SvReplicate sends a replication Interest to targetServerID, applying
// key/value as a replicated entry on the remote server. An empty value signals
// a replicated deletion.
func (s *Server) SvReplicate(targetServerID, key, value string) (*Response, error) {
	name, err := SvReplicateName(targetServerID, key)
	if err != nil {
		return nil, fmt.Errorf("failed to build SV-REPLICATE name: %w", err)
	}
	var payload enc.Wire
	if value != "" {
		payload = enc.Wire{[]byte(value)}
	}
	return s.express(name, payload)
}

// express sends an outbound Interest from this server and waits for a Data
// response. Used for server-to-server operations (SvTransfer, SvReplicate).
func (s *Server) express(name enc.Name, appParam enc.Wire) (*Response, error) {
	intCfg := &ndnlib.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(8 * time.Second),
		Nonce:       utils.ConvertNonce(s.engine.Timer().Nonce()),
	}

	interest, err := s.engine.Spec().MakeInterest(name, intCfg, appParam, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Interest: %w", err)
	}

	type result struct {
		resp *Response
		err  error
	}

	ch := make(chan result, 1)

	err = s.engine.Express(interest, func(args ndnlib.ExpressCallbackArgs) {
		switch args.Result {
		case ndnlib.InterestResultData:
			content := args.Data.Content().Join()
			var resp Response
			if err := json.Unmarshal(content, &resp); err != nil {
				ch <- result{err: fmt.Errorf("failed to parse response: %w", err)}
				return
			}
			ch <- result{resp: &resp}
		case ndnlib.InterestResultNack:
			ch <- result{err: fmt.Errorf("Interest NACKed (reason=%d)", args.NackReason)}
		case ndnlib.InterestResultTimeout:
			ch <- result{err: fmt.Errorf("Interest timed out for %s", name)}
		case ndnlib.InterestCancelled:
			ch <- result{err: fmt.Errorf("Interest cancelled for %s", name)}
		default:
			ch <- result{err: fmt.Errorf("unexpected result %d for %s", args.Result, name)}
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to express Interest: %w", err)
	}

	r := <-ch
	return r.resp, r.err
}

// handleSvTransfer processes an inbound sv-transfer Interest.
// The sending server is transferring authoritative ownership of a key to this server.
// An empty AppParam signals a deletion.
func (s *Server) handleSvTransfer(args ndnlib.InterestHandlerArgs, name enc.Name, key string, appParam enc.Wire) {
	if key == "" {
		s.replyJSON(args, name, Response{Status: StatusError, Error: "missing_key"})
		return
	}

	value := string(appParam.Join())
	if value == "" {
		if err := s.store.Del(key); err != nil {
			log.Errorf("store.Del (sv-transfer) failed: %v", err)
			s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
			return
		}
	} else {
		if err := s.store.Set(key, value); err != nil {
			log.Errorf("store.Set (sv-transfer) failed: %v", err)
			s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
			return
		}
	}

	log.Debugf("sv-transfer applied: key=%q", key)
	s.replyJSON(args, name, Response{Status: StatusSuccess, Key: key})
}

// handleSvReplicate processes an inbound sv-replicate Interest.
// The coordinator is shipping a log entry to this replica node.
// An empty AppParam signals a replicated deletion.
func (s *Server) handleSvReplicate(args ndnlib.InterestHandlerArgs, name enc.Name, key string, appParam enc.Wire) {
	if key == "" {
		s.replyJSON(args, name, Response{Status: StatusError, Error: "missing_key"})
		return
	}

	value := string(appParam.Join())
	if value == "" {
		if err := s.store.DelReplicated(key); err != nil {
			log.Errorf("store.DelReplicated (sv-replicate) failed: %v", err)
			s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
			return
		}
	} else {
		if err := s.store.SetReplicated(key, value); err != nil {
			log.Errorf("store.SetReplicated (sv-replicate) failed: %v", err)
			s.replyJSON(args, name, Response{Status: StatusError, Error: "internal"})
			return
		}
	}

	log.Debugf("sv-replicate applied: key=%q", key)
	s.replyJSON(args, name, Response{Status: StatusSuccess, Key: key})
}

// responsibleFor checks if this server owns the key based on metadata.
// isWrite=true for write operations, false for reads.
func (s *Server) responsibleFor(key string, isWrite bool) bool {
	s.mu.Lock()
	meta := s.metadata
	s.mu.Unlock()

	if len(meta) == 0 {
		return true
	}

	for _, kr := range meta {
		if kr.Covers(key) {
			if !kr.IsReplica() {
				return true
			}
			if isWrite {
				return kr.Replica.Write
			}
			return kr.Replica.Read
		}
	}

	hash := util.MD5HashUint128(key)
	hashH := util.Uint128BigEndian(hash)
	log.Warnf("NDN server %s not responsible for key %s hash(hex): %s", s.serverID, key, hashH)

	return false
}

// replyJSON serializes a Response as JSON and sends it as an NDN Data packet.
func (s *Server) replyJSON(args ndnlib.InterestHandlerArgs, name enc.Name, resp Response) {
	payload, err := json.Marshal(resp)
	if err != nil {
		log.Errorf("failed to marshal response: %v", err)
		return
	}

	dataPkt, err := s.engine.Spec().MakeData(
		name,
		&ndnlib.DataConfig{
			ContentType: optional.Some(ndnlib.ContentTypeBlob),
			Freshness:   optional.Some(1 * time.Millisecond),
		},
		enc.Wire{payload},
		s.signer,
	)
	if err != nil {
		log.Errorf("failed to encode Data packet: %v", err)
		return
	}

	if err := args.Reply(dataPkt.Wire); err != nil {
		log.Errorf("failed to send Data reply: %v", err)
		return
	}

	log.Debugf("<< Data: %s (%s)", name, resp.Status)
}
