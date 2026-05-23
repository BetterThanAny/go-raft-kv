package storage

type Operation string

const (
	OpPut    Operation = "put"
	OpDelete Operation = "delete"
	OpCAS    Operation = "cas"
)

type Command struct {
	Op       Operation `json:"op"`
	Key      string    `json:"key"`
	Value    string    `json:"value,omitempty"`
	Expected string    `json:"expected,omitempty"`
}

type ApplyResult struct {
	OK      bool   `json:"ok"`
	Found   bool   `json:"found,omitempty"`
	Value   string `json:"value,omitempty"`
	Swapped bool   `json:"swapped,omitempty"`
	Error   string `json:"error,omitempty"`
}

type LogEntry struct {
	Index   uint64  `json:"index"`
	Term    uint64  `json:"term"`
	Command Command `json:"command"`
}

type HardState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for,omitempty"`
	CommitIndex uint64 `json:"commit_index"`
}

type Snapshot struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	Data              map[string]string `json:"data"`
}
