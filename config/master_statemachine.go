package config

import (
  "sync"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/master"
  "github.com/lichuang/gpaxos/util"
  "github.com/golang/protobuf/proto"
  "github.com/lichuang/gpaxos"

  log "github.com/lichuang/log4go"
)

// MasterOperatorType
const(
  MasterOperatorType_Complete = 1
)

type MasterStateMachine struct {
  InsideStateMachine
  myGroupIdx    int32
  myNodeId      uint64
  masterNodeId  uint64
  masterVersion uint64
  leaseTime     int32
  absExpireTime uint64
  mvStore       *master.MasterVariablesStore
  mutex         sync.Mutex
}

func NewMasterStateMachine(groupIdx int32, myNodeId uint64,
  storage *logstorage.LogStorage) *MasterStateMachine {
  return &MasterStateMachine{
    myGroupIdx: groupIdx,
    myNodeId:myNodeId,
    masterNodeId:common.NULL_NODEID,
    masterVersion:-1,
    leaseTime:0,
    absExpireTime:0,
    mvStore:master.NewMasterVariablesStore(storage),
  }
}

func (self *MasterStateMachine) Init() error {
  var variables common.MasterVariables
  err := self.mvStore.Read(self.myGroupIdx, &variables)
  if err != nil && err != common.ErrKeyNotFound {
    log.Error("Master variables read from store fail %v", err)
    return err
  }

  if err == common.ErrKeyNotFound {
    log.Info("no master variables exist")
  } else {
    self.masterVersion = variables.GetVersion()
    if variables.GetVersion() == self.myNodeId {
      self.masterNodeId = common.NULL_NODEID
      self.absExpireTime = 0
    } else {
      self.masterNodeId = variables.GetMasterNodeid()
      self.absExpireTime = util.NowTimeMs() + uint64(variables.GetLeaseTime())
    }
  }

  log.Info("OK, master nodeid %d version %d expiretime %d",
    self.masterNodeId, self.masterVersion, self.absExpireTime)
  return nil
}

func (self *MasterStateMachine) UpdateMasterToStore(masterNodeId uint64, version uint64,
  leaseTime int32) error {
  variables := common.MasterVariables{
    MasterNodeid:proto.Uint64(masterNodeId),
    Version:proto.Uint64(version),
    LeaseTime:proto.Uint32(uint32(leaseTime)),
  }

  options := logstorage.WriteOptions{
    Sync:false,
  }

  return self.mvStore.Write(options, self.myGroupIdx, variables)
}

func (self *MasterStateMachine) LearnMaster(instance uint64, operator master.MasterOperator,
  absMasterTimeout uint64) error {
  self.mutex.Lock()
  defer self.mutex.Unlock()

  if operator.GetVersion() != self.masterVersion {
    log.Error("version conflit, op version %d now master version %d",
      operator.GetVersion(), self.masterVersion)

    return nil
  }

  err := self.UpdateMasterToStore(operator.GetNodeid(), instance, operator.GetTimeout())
  if err != nil {
    log.Error("UpdateMasterToStore fail %v", err)
    return err
  }

  self.masterNodeId = operator.GetNodeid()
  if self.masterNodeId == self.myNodeId {
    self.absExpireTime = absMasterTimeout
    log.Info("Be master success, absexpiretime %d", self.absExpireTime)
  } else {
    self.absExpireTime = util.NowTimeMs() + uint64(operator.GetTimeout())
    log.Info("Other be master, absexpiretime %d", self.absExpireTime)
  }

  self.leaseTime = operator.GetTimeout()
  self.masterVersion = instance

  log.Info("OK, masternodeid %d version %d abstimeout %d",
    self.masterNodeId, self.masterVersion, self.absExpireTime)

  return nil
}

func (self *MasterStateMachine) SafeGetMaster(masterNodeId *uint64, masterVersion *uint64) {
  self.mutex.Lock()

  if util.NowTimeMs() >= self.absExpireTime {
    *masterNodeId = common.NULL_NODEID
  } else {
    *masterNodeId = self.masterNodeId
  }
  *masterVersion = self.masterVersion
  self.mutex.Unlock()
}

func (self *MasterStateMachine) GetMaster() uint64 {
  if util.NowTimeMs() >= self.absExpireTime {
    return common.NULL_NODEID
  }

  return self.masterNodeId
}

func (self *MasterStateMachine)GetMasterWithVersion(version *uint64) uint64 {
  masterNodeId := common.NULL_NODEID
  self.SafeGetMaster(&masterNodeId, version)
  return masterNodeId
}

func (self *MasterStateMachine) IsIMMaster() bool {
  return self.GetMaster() == self.myNodeId
}

func (self *MasterStateMachine) Execute(groupIdx int32, instanceId uint64, value []byte,
  ctx *gpaxos.StateMachineContext) error {
  var operator master.MasterOperator
  err := proto.Unmarshal(value, &operator)
  if err != nil {
    log.Error("oMasterOper data wrong %v", err)
    return err
  }

  if operator.GetOperator() == MasterOperatorType_Complete {
    var absMasterTimeout uint64 = 0
    if ctx != nil && ctx.Context != nil {
      absMasterTimeout = *(ctx.Context.(*uint64))
    }

    log.Info("absmaster timeout %v", absMasterTimeout)

    err = self.LearnMaster(instanceId, operator, absMasterTimeout)
    if err != nil {
      return err
    }
  } else {
    log.Error("unknown op %d", operator.GetOperator())
    return nil
  }

  return nil
}

func (self *MasterStateMachine) MakeOpValue(nodeId uint64, version uint64,
  timeout int32, op uint32)([]byte, error) {
  operator := master.MasterOperator{
    Nodeid:proto.Uint64(nodeId),
    Version:proto.Uint64(version),
    Timeout:proto.Int32(timeout),
    Operator:proto.Uint32(op),
    Sid:proto.Uint32(util.Rand()),
  }

  return proto.Marshal(operator)
}

func (self *MasterStateMachine) GetCheckpointBuffer()([]byte, error) {
  if self.masterVersion == -1 {
    return nil,nil
  }

  v := common.MasterVariables {
    MasterNodeid:proto.Uint64(self.masterNodeId),
    Version:proto.Uint64(self.masterVersion),
    LeaseTime:proto.Uint32(uint32(self.leaseTime)),
  }

  return proto.Marshal(v)
}

func (self *MasterStateMachine) UpdateByCheckpoint(buffer []byte, change *bool) error {
  if len(buffer) == 0 {
    return nil
  }

  var variables common.MasterVariables
  err := proto.Unmarshal(buffer, &variables)
  if err != nil {
    log.Error("Variables.ParseFromArray fail: %v", err)
    return err
  }

  if variables.GetVersion() <= self.masterVersion && self.masterVersion != -1 {
    log.Info("lag checkpoint, no need update, cp.version %d now.version %d",
      variables.GetVersion(), self.masterVersion)
    return nil
  }

  err = self.UpdateMasterToStore(variables.GetMasterNodeid(), variables.GetVersion(),int32(variables.GetLeaseTime()))
  if err != nil {
    return err
  }

  log.Info("ok, cp.version %d cp.masternodeid %d old.version %d old.masternodeid %d",
    variables.GetVersion(), variables.GetMasterNodeid(), self.masterVersion, self.masterNodeId)

  self.masterVersion = variables.GetVersion()
  if variables.GetMasterNodeid() == self.myNodeId {
    self.masterNodeId = common.NULL_NODEID
    self.absExpireTime = 0
  } else {
    self.masterNodeId = variables.GetMasterNodeid()
    self.absExpireTime = util.NowTimeMs() + uint64(variables.GetLeaseTime())
  }

  return nil
}