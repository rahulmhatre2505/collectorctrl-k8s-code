// pkg/opamp/client.go
// Shared OpAMP client library for both CollectorCtrl Server and K8s Operator.
// This is a NEW package. If the server has existing OpAMP code, migrate it here
// or wrap it. The key requirement: both sides import the same message types.

package opamp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/collectorctrl/collectorctrl/pkg/api"
)

// MessageType identifies the kind of OpAMP message.
type MessageType string

const (
	MsgAgentToServer      MessageType = "AgentToServer"
	MsgServerToAgent      MessageType = "ServerToAgent"
	MsgEmergencyConfig    MessageType = "EmergencyConfig"
	MsgEmergencyAck       MessageType = "EmergencyAck"
	MsgHealthReport       MessageType = "HealthReport"
	MsgDriftAlert         MessageType = "DriftAlert"
)

// ClientConfig configures the OpAMP client.
type ClientConfig struct {
	Endpoint    string
	Headers     map[string]string // e.g., Authorization: Secret-Key xxx
	TLSConfig   *tls.Config
	AgentID     string
	AgentType   api.AgentType
	Labels      map[string]string
	K8sContext  *api.K8sContext // nil for VM agents
}

// Client is an OpAMP client that maintains a persistent WebSocket connection.
type Client struct {
	config ClientConfig

	conn      *websocket.Conn
	connMu    sync.RWMutex
	stopCh    chan struct{}
	doneCh    chan struct{}

	// Handlers registered by the consumer (server or operator).
	onConfigUpdate  func(ConfigUpdate)
	onEmergencyCmd  func(EmergencyCommand)
	onServerStatus  func(ServerStatus)

	lastHealth api.HealthStatus
	healthMu   sync.RWMutex
}

// ConfigUpdate is sent by the server to push a new configuration.
type ConfigUpdate struct {
	ConfigYAML       string            `json:"configYaml"`
	ConfigHash       string            `json:"configHash"`
	RolloutStrategy  *api.RolloutStrategy `json:"rolloutStrategy,omitempty"`
}

// EmergencyCommand is sent by the server to force an immediate config change.
type EmergencyCommand struct {
	ConfigYAML string `json:"configYaml"`
	Reason     string `json:"reason"` // human-readable reason for emergency
}

// ServerStatus carries server-side metadata (e.g., server version, capabilities).
type ServerStatus struct {
	ServerVersion string `json:"serverVersion"`
}

// AgentToServer is the message sent periodically by the client.
type AgentToServer struct {
	Type          MessageType           `json:"type"`
	AgentID       string                `json:"agentId"`
	AgentType     api.AgentType         `json:"agentType"`
	Labels        map[string]string     `json:"labels"`
	K8sContext    *api.K8sContext       `json:"k8sContext,omitempty"`
	Version       string                `json:"version,omitempty"`
	EffectiveConfigHash string          `json:"effectiveConfigHash,omitempty"`
	Health        api.HealthStatus      `json:"health"`
	Metrics       map[string]float64    `json:"metrics,omitempty"`
	Timestamp     time.Time             `json:"timestamp"`
}

// ServerToAgent is the message sent by the server.
type ServerToAgent struct {
	Type         MessageType      `json:"type"`
	ConfigUpdate *ConfigUpdate    `json:"configUpdate,omitempty"`
	Emergency    *EmergencyCommand `json:"emergency,omitempty"`
	Status       *ServerStatus    `json:"status,omitempty"`
}

// NewClient creates an OpAMP client.
func NewClient(cfg ClientConfig) *Client {
	return &Client{
		config: cfg,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// OnConfigUpdate registers a handler for normal config pushes.
func (c *Client) OnConfigUpdate(fn func(ConfigUpdate)) {
	c.onConfigUpdate = fn
}

// OnEmergencyCmd registers a handler for emergency override commands.
func (c *Client) OnEmergencyCmd(fn func(EmergencyCommand)) {
	c.onEmergencyCmd = fn
}

// OnServerStatus registers a handler for server status messages.
func (c *Client) OnServerStatus(fn func(ServerStatus)) {
	c.onServerStatus = fn
}

// Start establishes the WebSocket connection and begins the heartbeat loop.
func (c *Client) Start(ctx context.Context) error {
	dialer := websocket.Dialer{
		TLSClientConfig: c.config.TLSConfig,
		HandshakeTimeout: 10 * time.Second,
	}

	headers := http.Header{}
	for k, v := range c.config.Headers {
		headers.Set(k, v)
	}

	conn, _, err := dialer.DialContext(ctx, c.config.Endpoint, headers)
	if err != nil {
		return fmt.Errorf("opamp dial %s: %w", c.config.Endpoint, err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	go c.readLoop()
	go c.heartbeatLoop()

	return nil
}

// Stop closes the connection and waits for goroutines to exit.
func (c *Client) Stop() {
	close(c.stopCh)
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()
	<-c.doneCh
}

// SendAgentReport sends the periodic status update to the server.
func (c *Client) SendAgentReport(report AgentToServer) error {
	c.healthMu.RLock()
	c.lastHealth = report.Health
	c.healthMu.RUnlock()

	return c.send(report)
}

// SendDriftAlert reports a config drift event.
func (c *Client) SendDriftAlert(podName, expectedHash, actualHash string) error {
	msg := AgentToServer{
		Type:      MsgDriftAlert,
		AgentID:   c.config.AgentID,
		AgentType: c.config.AgentType,
		Labels: map[string]string{
			"drift.pod":     podName,
			"drift.expected": expectedHash,
			"drift.actual":   actualHash,
		},
		Timestamp: time.Now().UTC(),
	}
	return c.send(msg)
}

// SendEmergencyAck acknowledges an emergency command.
func (c *Client) SendEmergencyAck(success bool, errMsg string) error {
	msg := AgentToServer{
		Type:    MsgEmergencyAck,
		AgentID: c.config.AgentID,
		Labels: map[string]string{
			"emergency.success": fmt.Sprintf("%v", success),
			"emergency.error":   errMsg,
		},
		Timestamp: time.Now().UTC(),
	}
	return c.send(msg)
}

// send writes a JSON message to the WebSocket.
func (c *Client) send(v interface{}) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return fmt.Errorf("opamp: not connected")
	}

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return conn.WriteMessage(websocket.TextMessage, data)
}

// readLoop handles incoming server messages.
func (c *Client) readLoop() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		c.connMu.RLock()
		conn := c.conn
		c.connMu.RUnlock()

		if conn == nil {
			time.Sleep(time.Second)
			continue
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			// TODO: implement exponential backoff reconnect
			continue
		}

		var msg ServerToAgent
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case MsgServerToAgent:
			if msg.ConfigUpdate != nil && c.onConfigUpdate != nil {
				c.onConfigUpdate(*msg.ConfigUpdate)
			}
			if msg.Status != nil && c.onServerStatus != nil {
				c.onServerStatus(*msg.Status)
			}
		case MsgEmergencyConfig:
			if msg.Emergency != nil && c.onEmergencyCmd != nil {
				c.onEmergencyCmd(*msg.Emergency)
			}
		}
	}
}

// heartbeatLoop sends periodic keep-alive reports.
func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.healthMu.RLock()
			health := c.lastHealth
			c.healthMu.RUnlock()

			report := AgentToServer{
				Type:      MsgAgentToServer,
				AgentID:   c.config.AgentID,
				AgentType: c.config.AgentType,
				Labels:    c.config.Labels,
				K8sContext: c.config.K8sContext,
				Health:    health,
				Timestamp: time.Now().UTC(),
			}
			_ = c.send(report)
		}
	}
}
