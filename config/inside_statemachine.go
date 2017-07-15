package config

import (
  "github.com/lichuang/gpaxos"
)

type InsideStateMachine interface {
  gpaxos.StateMachine

  GetCheckpointBuffer()([]byte, error)

  UpdateByCheckpoint(buffer []byte, change *bool) error
}
