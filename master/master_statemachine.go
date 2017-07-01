package master

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/golang/protobuf/proto"
  "github.com/lichuang/gpaxos/util"
  "sync"
  "github.com/lichuang/gpaxos"
)

const MasterOperatorType_Complete = 1

type MasterStatemachine struct {
  config.InsideStateMachine

  MyGroupIdx int32
  MyNodeId uint64
  MasterNodeId uint64
  MasterVersion uint64
  LeaseTime uint32
  AbsExpireTime uint64
  MVStore *MasterVariablesStore
  Mutex sync.Mutex
}

func NewMasterStatemachine(storage logstorage.LogStorage, myNodeId uint64, groupIdx int32) *MasterStatemachine {
  return &MasterStatemachine{
    MyGroupIdx:groupIdx,
    MyNodeId:myNodeId,
    MVStore:NewMasterVariablesStore(storage),
    MasterNodeId:common.NULL_NODEID,
    MasterVersion:uint64(-1),
  }
}

func (self *MasterStatemachine)Init() error {
  var variables common.MasterVariables
  err := self.MVStore.Read(self.MyGroupIdx, &variables)
  if err != nil && err != common.ErrKeyNotFound {
    log.Error("Master variables read from store fail:%v", err)
    return err
  }

  if err == common.ErrKeyNotFound {
    log.Info("no master variables exist")
  } else {
    self.MasterVersion = variables.GetVersion()
    if variables.GetMasterNodeid() == self.MyNodeId {
      self.MasterNodeId = common.NULL_NODEID
      self.AbsExpireTime = 0
    } else {
      self.MasterNodeId = variables.GetMasterNodeid()
      self.AbsExpireTime = uint64(variables.GetLeaseTime()) + util.NowTimeMs()
    }
  }

  log.Info("OK, master nodeid %d version %d expiretime %d", self.MasterNodeId, self.MasterVersion, self.AbsExpireTime)
  return nil
}

func (self *MasterStatemachine)UpdateMasterToStore(masterNodeId uint64, version uint64, leasetime uint32) error {
  variables := common.MasterVariables{
    MasterNodeid:proto.Uint64(masterNodeId),
    Version:proto.Uint64(version),
    LeaseTime:proto.Uint32(leasetime),
  }

  options := logstorage.WriteOptions{
    Sync:true,
  }

  return self.MVStore.Write(options, self.MyGroupIdx, variables)
}

func (self *MasterStatemachine) LearnMaster(instanceId uint64, operator MasterOperator, absMasterTimeout uint64) error {
  self.Mutex.Lock()
  defer self.Mutex.Unlock()

  if operator.GetVersion() != self.MasterVersion {
    log.Error("version conflit, op version %d now master version %d", operator.GetVersion(), self.MasterVersion)
    return nil
  }

  err := self.UpdateMasterToStore(operator.GetNodeid(), instanceId, uint32(operator.GetTimeout()))
  if err != nil {
    log.Error("UpdateMasterToStore fail:%v", err)
    return err
  }

  self.MasterNodeId = operator.GetNodeid()
  if self.MasterNodeId == self.MyNodeId {
    self.AbsExpireTime = absMasterTimeout

    log.Info("Be master success, absexpiretime %d", self.AbsExpireTime)
  } else {
    self.AbsExpireTime = util.NowTimeMs() + uint64(operator.GetTimeout())

    log.Info("other be maste, absexpiretime %d", self.AbsExpireTime)
  }

  self.LeaseTime = uint32(operator.GetTimeout())
  self.MasterVersion = instanceId

  log.Info("OK, masternodeid %d version %d abstimeout %d",
    self.MasterNodeId, self.MasterVersion, self.AbsExpireTime)

  return nil
}

func (self *MasterStatemachine)SafeGetMaster()(masterNodeId uint64, masterVersion uint64) {
  self.Mutex.Lock()

  if util.NowTimeMs() >= self.AbsExpireTime {
    masterNodeId = common.NULL_NODEID
  } else {
    masterNodeId = self.MasterNodeId
  }

  masterVersion = self.MasterVersion

  self.Mutex.Unlock()

  return
}

func (self *MasterStatemachine)GetMaster()(masterNodeId uint64) {

  if util.NowTimeMs() >= self.AbsExpireTime {
    masterNodeId = common.NULL_NODEID
  } else {
    masterNodeId = self.MasterNodeId
  }

  return
}

func (self *MasterStatemachine)GetMasterWithVersion() (masterNodeId uint64, masterVersion uint64){
  return self.SafeGetMaster()
}

func (self *MasterStatemachine) IsImMaster() bool {
  return self.MyNodeId == self.GetMaster()
}

func (self *MasterStatemachine) Execute(groupIdx int32, instanceId uint64,
                                        value[]byte, ctx *gpaxos.StateMachineContext) bool {
  var operator MasterOperator
  err := proto.Unmarshal(value, &operator)
  if err != nil {
    log.Error("master operator unmarshal fail:%v", err)
    return false
  }

  if operator.GetOperator() == MasterOperatorType_Complete {
    var absMasterTimeout uint64
    if ctx != nil && ctx.Context != nil {
      absMasterTimeout =  uint64(ctx.Context)
    }

    err = self.LearnMaster(instanceId, operator, absMasterTimeout)
    if err != nil {
      return false
    }
  } else {
    log.Error("unknown op %d", operator.GetOperator())
    return true
  }

  return true
}

func (self *MasterStatemachine)MakeOpValue(nodeId uint64, version uint64, timeout int32,
                                           opType uint32, value []byte) error {
  operator := MasterOperator{
    Nodeid:proto.Uint64(nodeId),
    Version:proto.Uint64(version),
    Timeout:proto.Int32(timeout),
    Operator:proto.Uint32(opType),
    Sid:proto.Uint32(util.Rand()),
  }

  value, err := proto.Marshal(operator)
  return err
}

func (self *MasterStatemachine)GetCheckpointBuffer(buffer []byte) error {
  if self.MasterVersion == uint64(-1) {
    return nil
  }

  variables := common.MasterVariables{
    MasterNodeid:proto.Uint64(self.MasterNodeId),
    Version:proto.Uint64(self.MasterVersion),
    LeaseTime:proto.Uint32(self.LeaseTime),
  }

  buffer, err := proto.Marshal(variables)
  if err != nil {
    log.Error("Variables.Serialize fail:%v", err)
  }

  return err
}

func (self *MasterStatemachine)UpdateByCheckpoint(buffer[]byte) error {
  if len(buffer) == 0 {
    return nil
  }

  var variables common.MasterVariables
  err := proto.Unmarshal(buffer, &variables)
  if err != nil {
    log.Error("Variables.ParseFromArray fail:%v", err)
    return err
  }

  if variables.GetVersion() <= self.MasterVersion && self.MasterVersion != uint64(-1) {
    log.Info("lag checkpoint, no need update, cp.version %d now.version %d",
      variables.GetVersion(), self.MasterVersion)
    return nil
  }

  err = self.UpdateMasterToStore(variables.GetMasterNodeid(), variables.GetVersion(), variables.GetLeaseTime())
  if err != nil {
    return err
  }

  log.Info("ok, cp.version %d cp.masternodeid %d old.version %d old.masternodeid %d",
    variables.GetVersion(), variables.GetMasterNodeid(), self.MasterVersion, self.MasterNodeId)

  self.MasterVersion = variables.GetVersion()
  if variables.GetMasterNodeid() == self.MyNodeId {
    self.MasterNodeId = common.NULL_NODEID
    self.AbsExpireTime = 0
  } else {
    self.MasterNodeId = variables.GetMasterNodeid()
    self.AbsExpireTime = util.NowTimeMs() + uint64(variables.GetLeaseTime())
  }

  return nil
}