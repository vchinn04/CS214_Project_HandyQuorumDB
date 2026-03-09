package ndn

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/nStangl/distributed-kv-store/protocol"
	"github.com/nStangl/distributed-kv-store/server/store"
)

// TransferRange sends every key in the local store that falls within keyRange
// to targetServerID via NDN sv-transfer Interests.
//
// The server's embedded forwarder must already have a face and FIB route
// reaching the target server (e.g. established via AddUpstreamRoute before
// calling this function).
func TransferRange(srv *Server, st store.Store, targetServerID string, keyRange protocol.KeyRange) error {
	all, err := st.Flatten()
	if err != nil {
		return fmt.Errorf("failed to flatten local store: %w", err)
	}

	sent := 0

	for key, value := range all {
		if !keyRange.Covers(key) {
			continue
		}

		resp, err := srv.SvTransfer(targetServerID, key, value)
		if err != nil {
			return fmt.Errorf("sv-transfer failed for key %q: %w", key, err)
		}
		if resp.Status != StatusSuccess {
			return fmt.Errorf("sv-transfer rejected for key %q: status=%s error=%s",
				key, resp.Status, resp.Error)
		}
		sent++
	}

	log.Infof("TransferRange: sent %d keys to server %s", sent, targetServerID)
	return nil
}
