// pkg/opamp/client.go
// OpAMP client wrapper using the official opamp-go library with protobuf.
// This replaces the previous JSON-over-WebSocket implementation.

package opamp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"

	opampclient "github.com/open-telemetry/opamp-go/client"
	opampcltypes "github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"

	"github.com/collectorctrl/collectorctrl/pkg/api"
)

// ClientConfig configures the fleet-level OpAMP client.
type ClientConfig struct {
	Endpoint   string
	Headers    map[string]string // e.g., Authorization: Secret-Key xxx
	TLSConfig  *tls.Config
	AgentID    string
	AgentType  api.AgentType
	Labels     map[string]string
	K8sContext *api.K8sContext // nil for VM agents
}

// Client wraps the official opamp-go client for fleet-level reporting.
type Client struct {
	config ClientConfig

	client    opampclient.OpAMPClient
	connected bool
	connMu    sync.RWMutex

	effectiveConfig string
	configMu        sync.RWMutex

	onConfigUpdate func(ConfigUpdate)
	onEmergencyCmd func(EmergencyCommand)
}

// ConfigUpdate is sent by the server to push a new configuration.
type ConfigUpdate struct {
	ConfigYAML      string               `json:"configYaml"`
	ConfigHash      string               `json:"configHash"`
	RolloutStrategy *api.RolloutStrategy `json:"rolloutStrategy,omitempty"`
}

// EmergencyCommand is sent by the server to force an immediate config change.
type EmergencyCommand struct {
	ConfigYAML string `json:"configYaml"`
	Reason     string `json:"reason"`
}

// NewClient creates a fleet-level OpAMP client.
func NewClient(cfg ClientConfig) *Client {
	return &Client{
		config: cfg,
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

// Start establishes the OpAMP connection.
func (c *Client) Start(ctx context.Context) error {
	c.client = opampclient.NewWebSocket(nil)

	// Build HTTP headers
	headers := make(http.Header)
	for k, v := range c.config.Headers {
		headers.Set(k, v)
	}

	// Build capabilities
	capabilities := protobufs.AgentCapabilities_AgentCapabilities_ReportsHealth |
		protobufs.AgentCapabilities_AgentCapabilities_ReportsEffectiveConfig |
		protobufs.AgentCapabilities_AgentCapabilities_ReportsStatus |
		protobufs.AgentCapabilities_AgentCapabilities_AcceptsRestartCommand

	settings := opampcltypes.StartSettings{
		OpAMPServerURL: c.config.Endpoint,
		TLSConfig:      c.config.TLSConfig,
		InstanceUid:    opampcltypes.InstanceUid(c.instanceUID()),
		Header:         headers,
		Callbacks: opampcltypes.Callbacks{
			OnConnect: func(ctx context.Context) {
				c.connMu.Lock()
				c.connected = true
				c.connMu.Unlock()
			},
			OnConnectFailed: func(ctx context.Context, err error) {
				_ = err
			},
			OnError: func(ctx context.Context, err *protobufs.ServerErrorResponse) {
				_ = err
			},
			GetEffectiveConfig: func(ctx context.Context) (*protobufs.EffectiveConfig, error) {
				return c.getEffectiveConfig(), nil
			},
			OnMessage: c.onMessage,
		},
		Capabilities: capabilities,
	}

	// Set agent description with K8s context
	if err := c.client.SetAgentDescription(c.agentDescription()); err != nil {
		return fmt.Errorf("set agent description: %w", err)
	}

	if err := c.client.Start(ctx, settings); err != nil {
		return fmt.Errorf("opamp start: %w", err)
	}

	return nil
}

// Stop closes the OpAMP connection.
func (c *Client) Stop() {
	if c.client != nil {
		_ = c.client.Stop(context.Background())
	}
}

// SetHealth updates the health status reported to the server.
func (c *Client) SetHealth(health api.HealthStatus, totalPods, readyPods int) error {
	if c.client == nil {
		return fmt.Errorf("client not started")
	}

	healthy := health == api.HealthHealthy
	componentHealthMap := map[string]*protobufs.ComponentHealth{
		"fleet": {
			Healthy:            healthy,
			StartTimeUnixNano:  0,
			StatusTimeUnixNano: 0,
			ComponentHealthMap: map[string]*protobufs.ComponentHealth{
				"total_pods": {
					Healthy: true,
					Status:  fmt.Sprintf("%d", totalPods),
				},
				"ready_pods": {
					Healthy: readyPods == totalPods,
					Status:  fmt.Sprintf("%d", readyPods),
				},
			},
		},
	}

	return c.client.SetHealth(&protobufs.ComponentHealth{
		Healthy:            healthy,
		ComponentHealthMap: componentHealthMap,
	})
}

// SetEffectiveConfig stores the config and triggers sending it to the server.
func (c *Client) SetEffectiveConfig(configYAML string) error {
	if c.client == nil {
		return fmt.Errorf("client not started")
	}

	c.configMu.Lock()
	c.effectiveConfig = configYAML
	c.configMu.Unlock()

	return c.client.UpdateEffectiveConfig(context.Background())
}

// SendEmergencyAck sends an emergency acknowledgement to the server.
func (c *Client) SendEmergencyAck(success bool, errMsg string) error {
	if c.client == nil {
		return fmt.Errorf("client not started")
	}

	data := fmt.Sprintf("emergency_ack:%v:%s", success, errMsg)
	_, err := c.client.SendCustomMessage(&protobufs.CustomMessage{
		Capability: "collectorctrl",
		Type:       "emergency_ack",
		Data:       []byte(data),
	})
	return err
}

// Connected returns true if the client is connected.
func (c *Client) Connected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connected
}

// instanceUID generates a stable instance UID from the agent ID.
func (c *Client) instanceUID() [16]byte {
	var uid [16]byte
	copy(uid[:], []byte(c.config.AgentID))
	return uid
}

// agentDescription builds the protobuf AgentDescription.
func (c *Client) agentDescription() *protobufs.AgentDescription {
	identifying := []*protobufs.KeyValue{
		keyVal("service.name", string(c.config.AgentType)),
		keyVal("service.instance.id", c.config.AgentID),
	}

	nonIdentifying := []*protobufs.KeyValue{}
	for k, v := range c.config.Labels {
		nonIdentifying = append(nonIdentifying, keyVal(k, v))
	}

	// Add K8s context as attributes
	if kc := c.config.K8sContext; kc != nil {
		nonIdentifying = append(nonIdentifying,
			keyVal("k8s.cluster.name", kc.ClusterName),
			keyVal("k8s.namespace", kc.Namespace),
			keyVal("k8s.workload.type", kc.WorkloadType),
			keyVal("k8s.workload.name", kc.WorkloadName),
			keyVal("k8s.configmap.name", kc.ConfigMapName),
			keyVal("k8s.configmap.key", kc.ConfigMapKey),
		)
	}

	return &protobufs.AgentDescription{
		IdentifyingAttributes:    identifying,
		NonIdentifyingAttributes: nonIdentifying,
	}
}

// getEffectiveConfig returns the current effective config protobuf.
func (c *Client) getEffectiveConfig() *protobufs.EffectiveConfig {
	c.configMu.RLock()
	cfg := c.effectiveConfig
	c.configMu.RUnlock()

	return &protobufs.EffectiveConfig{
		ConfigMap: &protobufs.AgentConfigMap{
			ConfigMap: map[string]*protobufs.AgentConfigFile{
				"": {
					Body:        []byte(cfg),
					ContentType: "text/yaml",
				},
			},
		},
	}
}

// onMessage handles incoming server messages.
func (c *Client) onMessage(ctx context.Context, msg *opampcltypes.MessageData) {
	if msg.RemoteConfig != nil && c.onConfigUpdate != nil {
		cfg := msg.RemoteConfig.Config
		if cfg != nil {
			for _, file := range cfg.GetConfigMap() {
				c.onConfigUpdate(ConfigUpdate{
					ConfigYAML: string(file.Body),
					ConfigHash: fmt.Sprintf("%x", msg.RemoteConfig.ConfigHash),
				})
				break
			}
		}
	}
}

// keyVal is a helper to build protobuf KeyValue.
func keyVal(key, value string) *protobufs.KeyValue {
	return &protobufs.KeyValue{
		Key: key,
		Value: &protobufs.AnyValue{
			Value: &protobufs.AnyValue_StringValue{StringValue: value},
		},
	}
}
