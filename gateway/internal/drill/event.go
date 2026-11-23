package drill

// Action 是演练事件的动作取值，直接序列化（D22）。
type Action string

const (
	ActionRemove  Action = "remove"
	ActionRecover Action = "recover"
	ActionReroute Action = "reroute"
)

// Event 是 /drill-events 的单条记录（D22）。
type Event struct {
	TsMs      int64  `json:"ts_ms"`
	Node      string `json:"node"`
	Action    Action `json:"action"`
	RequestID string `json:"request_id"`
}
