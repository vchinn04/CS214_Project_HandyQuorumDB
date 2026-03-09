package ndn

// ECSCmdType identifies the type of command sent from ECS to a server via poll.
type ECSCmdType string

const (
	ECSCmdNone           ECSCmdType = "none"
	ECSCmdMetadata       ECSCmdType = "metadata"
	ECSCmdSetWriteLock   ECSCmdType = "set_write_lock"
	ECSCmdLiftWriteLock  ECSCmdType = "lift_write_lock"
	ECSCmdInvokeTransfer ECSCmdType = "invoke_transfer"
	ECSCmdJoinComplete   ECSCmdType = "join_complete"
)

// ECSCommand is queued for delivery to a server via the poll mechanism.
type ECSCommand struct {
	Type     ECSCmdType `json:"type"`
	Metadata string     `json:"metadata,omitempty"`
	Address  string     `json:"address,omitempty"`
}

// ECSJoinRequest is sent by servers in the AppParam of a join Interest.
type ECSJoinRequest struct {
	ServerID string `json:"server_id"`
	NdnAddr  string `json:"ndn_addr"`
}

// ECSPollResponse is returned as Data content to a poll Interest.
type ECSPollResponse struct {
	Status  string      `json:"status"`
	Command *ECSCommand `json:"command,omitempty"`
}

// ECSGenericResponse is a simple status response used for join, ack, heartbeat, etc.
type ECSGenericResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}
