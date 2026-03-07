package ndn

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/named-data/ndnd/fw/cmd"
	"github.com/named-data/ndnd/fw/core"
)

// ForwarderConfig holds configuration for an embedded NDN forwarder.
type ForwarderConfig struct {
	// SocketPath is the Unix socket path for this forwarder.
	// Each server must have a unique path.
	SocketPath string
	// TCPPort is the TCP listener port (0 to disable).
	TCPPort uint16
	// UDPUnicastPort is the UDP unicast port (0 to disable).
	UDPUnicastPort uint16
	// LogLevel is the forwarder log level (default "WARN").
	LogLevel string
}

// DefaultForwarderConfig returns a ForwarderConfig with sensible defaults
// for a given server ID. Each server gets a unique socket in /tmp.
func DefaultForwarderConfig(serverID string, basePort uint16) ForwarderConfig {
	return ForwarderConfig{
		SocketPath:     filepath.Join(os.TempDir(), fmt.Sprintf("ndn-kv-%s.sock", serverID)),
		TCPPort:        basePort,
		UDPUnicastPort: basePort,
		LogLevel:       "WARN",
	}
}

// Forwarder wraps an embedded YaNFD instance.
type Forwarder struct {
	yanfd      *cmd.YaNFD
	socketPath string
}

// NewForwarder creates and configures an embedded NDN forwarder.
func NewForwarder(cfg ForwarderConfig) *Forwarder {
	c := core.DefaultConfig()

	c.Core.LogLevel = cfg.LogLevel

	// Unix socket — primary face for the co-located application engine
	c.Faces.Unix.Enabled = true
	c.Faces.Unix.SocketPath = cfg.SocketPath

	// TCP — for inter-server connectivity
	if cfg.TCPPort > 0 {
		c.Faces.Tcp.Enabled = true
		c.Faces.Tcp.PortUnicast = cfg.TCPPort
	} else {
		c.Faces.Tcp.Enabled = false
	}

	// UDP unicast — for inter-server connectivity
	if cfg.UDPUnicastPort > 0 {
		c.Faces.Udp.EnabledUnicast = true
		c.Faces.Udp.PortUnicast = cfg.UDPUnicastPort
	} else {
		c.Faces.Udp.EnabledUnicast = false
	}

	// Disable features not needed for a KV store
	c.Faces.Udp.EnabledMulticast = false
	c.Faces.WebSocket.Enabled = false
	c.Faces.HTTP3.Enabled = false

	// Lightweight forwarding config
	c.Fw.Threads = 4
	c.Tables.ContentStore.Capacity = 512

	return &Forwarder{
		yanfd:      cmd.NewYaNFD(c),
		socketPath: cfg.SocketPath,
	}
}

// Start launches the embedded forwarder. Non-blocking.
// Waits briefly for the Unix socket to become available.
func (f *Forwarder) Start() error {
	// Clean up stale socket from a previous run
	os.Remove(f.socketPath)

	f.yanfd.Start()

	// Wait for the Unix socket to appear (the engine needs it)
	for range 50 {
		if _, err := os.Stat(f.socketPath); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("forwarder socket %s did not appear after startup", f.socketPath)
}

// Stop shuts down the forwarder and cleans up the socket.
func (f *Forwarder) Stop() {
	f.yanfd.Stop()
	os.Remove(f.socketPath)
}

// SocketPath returns the Unix socket path used by this forwarder.
func (f *Forwarder) SocketPath() string {
	return f.socketPath
}
