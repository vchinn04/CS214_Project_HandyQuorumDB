package ndn

import (
	"fmt"
	"strings"
	"time"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/utils"
)

// NDN naming scheme for the KV store:
//
//   /kv/<server-id>/get/<key>
//   /kv/<server-id>/put/<key>/<timestamp>       (value in AppParam)
//   /kv/<server-id>/delete/<key>/<timestamp>
//   /kv/<server-id>/keyrange/<timestamp>
//   /kv/<server-id>/keyrange-read/<timestamp>

const (
	PrefixKV = "/kv"

	OpGet          = "get"
	OpPut          = "put"
	OpDelete       = "delete"
	OpKeyrange     = "keyrange"
	OpKeyrangeRead = "keyrange-read"

	// Server-to-server operations
	OpSvTransfer  = "sv-transfer"  // authoritative data transfer during rebalancing
	OpSvReplicate = "sv-replicate" // async replication to replica nodes
)

// ServerPrefix returns the NDN name prefix for a specific server.
//
//	/kv/<server-id>
func ServerPrefix(serverID string) (enc.Name, error) {
	return enc.NameFromStr(fmt.Sprintf("%s/%s", PrefixKV, serverID))
}

// GetName returns an Interest name for a GET request.
//
//	/kv/<server-id>/get/<key>
func GetName(serverID, key string) (enc.Name, error) {
	return enc.NameFromStr(fmt.Sprintf("%s/%s/%s/%s", PrefixKV, serverID, OpGet, key))
}

// PutName returns an Interest name for a PUT request.
// Includes a timestamp to ensure uniqueness (preventing cache hits for writes).
//
//	/kv/<server-id>/put/<key>/<timestamp>
func PutName(serverID, key string) (enc.Name, error) {
	base, err := enc.NameFromStr(fmt.Sprintf("%s/%s/%s/%s", PrefixKV, serverID, OpPut, key))
	if err != nil {
		return nil, err
	}
	return base.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now()))), nil
}

// DeleteName returns an Interest name for a DELETE request.
//
//	/kv/<server-id>/delete/<key>/<timestamp>
func DeleteName(serverID, key string) (enc.Name, error) {
	base, err := enc.NameFromStr(fmt.Sprintf("%s/%s/%s/%s", PrefixKV, serverID, OpDelete, key))
	if err != nil {
		return nil, err
	}
	return base.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now()))), nil
}

// KeyrangeName returns an Interest name for a keyrange request.
//
//	/kv/<server-id>/keyrange/<timestamp>
func KeyrangeName(serverID string) (enc.Name, error) {
	base, err := enc.NameFromStr(fmt.Sprintf("%s/%s/%s", PrefixKV, serverID, OpKeyrange))
	if err != nil {
		return nil, err
	}
	return base.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now()))), nil
}

// KeyrangeReadName returns an Interest name for a keyrange-read request.
//
//	/kv/<server-id>/keyrange-read/<timestamp>
func KeyrangeReadName(serverID string) (enc.Name, error) {
	base, err := enc.NameFromStr(fmt.Sprintf("%s/%s/%s", PrefixKV, serverID, OpKeyrangeRead))
	if err != nil {
		return nil, err
	}
	return base.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now()))), nil
}

// SvTransferName returns an Interest name for a server-to-server transfer.
//
//	/kv/<server-id>/sv-transfer/<key>/<timestamp>
func SvTransferName(serverID, key string) (enc.Name, error) {
	base, err := enc.NameFromStr(fmt.Sprintf("%s/%s/%s/%s", PrefixKV, serverID, OpSvTransfer, key))
	if err != nil {
		return nil, err
	}
	return base.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now()))), nil
}

// SvReplicateName returns an Interest name for async replication to a replica.
//
//	/kv/<server-id>/sv-replicate/<key>/<timestamp>
func SvReplicateName(serverID, key string) (enc.Name, error) {
	base, err := enc.NameFromStr(fmt.Sprintf("%s/%s/%s/%s", PrefixKV, serverID, OpSvReplicate, key))
	if err != nil {
		return nil, err
	}
	return base.Append(enc.NewTimestampComponent(utils.MakeTimestamp(time.Now()))), nil
}

// ParsedInterest holds the parsed components of an incoming KV Interest name.
type ParsedInterest struct {
	ServerID string
	Op       string
	Key      string
}

// ParseInterestName extracts the operation and key from an incoming Interest name.
// Expected format: /kv/<server-id>/<op>[/<key>[/<timestamp>]]
func ParseInterestName(name enc.Name) (ParsedInterest, error) {
	str := name.String()
	parts := strings.Split(strings.TrimPrefix(str, "/"), "/")

	// Minimum: /kv/<server-id>/<op> = 3 parts
	if len(parts) < 3 || parts[0] != "kv" {
		return ParsedInterest{}, fmt.Errorf("invalid KV interest name: %s", str)
	}

	parsed := ParsedInterest{
		ServerID: parts[1],
		Op:       parts[2],
	}

	if len(parts) >= 4 {
		parsed.Key = parts[3]
	}

	return parsed, nil
}
