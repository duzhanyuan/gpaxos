package master

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/golang/protobuf/proto"
)

type MasterStatemachine struct {

}

func NewMasterStatemachine(storage logstorage.LogStorage, myNodeId uint64, groupIdx int32) *MasterStatemachine {
  return &MasterStatemachine{

  }
}


