package server

import (
	"encoding/json"

	"go-raft-kv/internal/storage"
)

func encodeCommand(command storage.Command) []byte {
	data, _ := json.Marshal(command)
	return data
}

func decodeCommand(data []byte) storage.Command {
	var command storage.Command
	_ = json.Unmarshal(data, &command)
	return command
}
