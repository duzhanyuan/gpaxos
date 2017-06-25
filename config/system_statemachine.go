package config

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/golang/protobuf/proto"
)

type SystemStateMachine struct {
  MyGroupIdx int32
  MyNodeId uint64
  systemVariables common.SystemVariables
  systemStore logstorage.SystemVariablesStore

  NodeIdMap map[uint64]bool

}

func (self *SystemStateMachine) GetCheckpointBuffer(buffer []byte) error {
  return nil
}
