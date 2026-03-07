package ndn

import (
	"fmt"

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/engine/face"
	ndnlib "github.com/named-data/ndnd/std/ndn"
	mgmt "github.com/named-data/ndnd/std/ndn/mgmt_2022"
	"github.com/named-data/ndnd/std/security/signer"
	"github.com/named-data/ndnd/std/types/optional"
)

// Config holds configuration for an NDN-based KV node.
type Config struct {
	// FaceType is "unix" or "tcp" (default "unix").
	FaceType string
	// FaceAddr is the NDN forwarder address.
	// For unix: socket path (e.g. /var/run/nfd/nfd.sock).
	// For tcp: host:port (e.g. localhost:6363).
	FaceAddr string
	// ServerID identifies this KV server in the NDN namespace.
	ServerID string
}

// NewEngine creates and starts an NDN engine connected to the local forwarder.
// If cfg.FaceAddr is empty, the default face from the NDN client config is used.
func NewEngine(cfg Config) (ndnlib.Engine, error) {
	var f ndnlib.Face
	switch cfg.FaceType {
	case "tcp":
		if cfg.FaceAddr == "" {
			cfg.FaceAddr = "localhost:6363"
		}
		f = face.NewStreamFace("tcp", cfg.FaceAddr, false)
	case "unix", "":
		if cfg.FaceAddr != "" {
			f = engine.NewUnixFace(cfg.FaceAddr)
		} else {
			f = engine.NewDefaultFace()
		}
	default:
		return nil, fmt.Errorf("unsupported NDN face type: %s", cfg.FaceType)
	}

	app := engine.NewBasicEngine(f)
	if err := app.Start(); err != nil {
		return nil, fmt.Errorf("failed to start NDN engine: %w", err)
	}

	return app, nil
}

// NewEngineForForwarder creates an NDN engine connected to an embedded forwarder's
// Unix socket. This is the standard path: each server runs its own forwarder and
// the engine connects to it via the local socket.
func NewEngineForForwarder(fw *Forwarder) (ndnlib.Engine, error) {
	f := engine.NewUnixFace(fw.SocketPath())
	app := engine.NewBasicEngine(f)
	if err := app.Start(); err != nil {
		return nil, fmt.Errorf("failed to start NDN engine on socket %s: %w", fw.SocketPath(), err)
	}
	return app, nil
}

// EngineCloser is ndn.Engine — exported so callers can hold a reference
// without importing the ndnd std library directly.
type EngineCloser = ndnlib.Engine

// AddUpstreamRoute creates a TCP face from the local forwarder to remoteURI
// (e.g. "tcp4://localhost:6363") and registers a FIB route for prefix over
// that face. This is required when the engine is connected to a local
// embedded forwarder and needs to forward Interests toward a remote forwarder.
func AddUpstreamRoute(eng ndnlib.Engine, prefix string, remoteURI string) error {
	// 1. Create (or reuse) a TCP face pointing at the remote forwarder.
	faceRaw, err := eng.ExecMgmtCmd("faces", "create", &mgmt.ControlArgs{
		Uri: optional.Some(remoteURI),
	})
	if err != nil {
		return fmt.Errorf("failed to create face to %s: %w", remoteURI, err)
	}

	faceRes, ok := faceRaw.(*mgmt.ControlResponse)
	if !ok || faceRes.Val == nil || faceRes.Val.Params == nil || !faceRes.Val.Params.FaceId.IsSet() {
		return fmt.Errorf("unexpected response creating face to %s", remoteURI)
	}
	faceID := faceRes.Val.Params.FaceId.Unwrap()

	// 2. Register the prefix route over that face.
	ndnPrefix, err := enc.NameFromStr(prefix)
	if err != nil {
		return fmt.Errorf("invalid NDN prefix %q: %w", prefix, err)
	}

	routeRaw, err := eng.ExecMgmtCmd("rib", "register", &mgmt.ControlArgs{
		Name:   ndnPrefix,
		FaceId: optional.Some(faceID),
	})
	if err != nil {
		return fmt.Errorf("failed to register route %s via face %d: %w", prefix, faceID, err)
	}

	routeRes, ok := routeRaw.(*mgmt.ControlResponse)
	if !ok || routeRes.Val == nil {
		return fmt.Errorf("unexpected response registering route %s", prefix)
	}

	// Status 200 = success, 409 = already exists (both are fine).
	if routeRes.Val.StatusCode != 200 && routeRes.Val.StatusCode != 409 {
		return fmt.Errorf("route registration %s returned status %d: %s",
			prefix, routeRes.Val.StatusCode, routeRes.Val.StatusText)
	}

	return nil
}

// NewSha256Signer returns a simple SHA256 digest signer.
// Suitable for testing; production deployments should use key-based signing.
func NewSha256Signer() ndnlib.Signer {
	return signer.NewSha256Signer()
}
