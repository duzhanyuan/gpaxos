package config

import (
  "github.com/lichuang/gpaxos"
)

type InsideStateMachine interface {
  gpaxos.StateMachine

  GetCheckpointBuffer(buffer []byte) error

  UpdateByCheckpoint(buffer []byte, change *bool) error
}
