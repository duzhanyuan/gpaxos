package sm_base

import (
  "github.com/lichuang/gpaxos"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/common"
  "github.com/golang/protobuf/proto"
)

type StateMachineFactory struct {
  MyGroupIdx int
  StateMachines []gpaxos.StateMachine
}

func NewStateMachineFactory(groupIdx int) *StateMachineFactory {
  return &StateMachineFactory{
    MyGroupIdx:groupIdx,
    StateMachines:make([]gpaxos.StateMachine, 0),
  }
}

func (self *StateMachineFactory) Execute(groupIdx int32, instanceId uint64,
                                         paxosValue[]byte, context *gpaxos.StateMachineContext) bool {
  valueLen := len(paxosValue)
  if valueLen < util.INT32SIZE {
    log.Error("value wrong, instance id %d size %d", instanceId, valueLen)
    return true
  }

  var smid int32
  util.DecodeInt32(paxosValue, 0, &smid)

  if smid == 0 {
    log.Error("value no need to do sm,instance id %d", instanceId)
    return true
  }

  bodyValue := paxosValue[util.INT32SIZE:]
  if smid == gpaxos.BATCH_PROPOSE_SMID {
    var batchCtx *gpaxos.BatchStateMachineContext
    if context != nil && context.Context != nil {
      batchCtx = context.Context.(*gpaxos.BatchStateMachineContext)
    }

    return self.BatchExecute(groupIdx, instanceId, bodyValue, batchCtx)
  } else {
    return self.DoExecute(groupIdx, instanceId, bodyValue, smid, context)
  }
}

func (self *StateMachineFactory) BatchExecute(groupIdx int32, instanceId uint64,
                                              bodyValue []byte, batchContext *gpaxos.BatchStateMachineContext) bool {
  var batchPaxosValues common.BatchPaxosValues
  err := proto.Unmarshal(bodyValue, &batchPaxosValues)

  if err != nil {
    log.Error("proto.Unmarshal error %v", err)
    return false
  }

  if batchContext != nil {
    if len(batchContext.StateMachineContexts) != len(batchPaxosValues.GetValues()) {
      return false
    }
  }

  for i, value := range batchPaxosValues.GetValues() {
    var context *gpaxos.StateMachineContext = nil
    if batchContext != nil {
      context = batchContext.StateMachineContexts[i]
    }

    success := self.DoExecute(groupIdx, instanceId, value.GetValue(), value.GetSMID(), context)
    if !success {
      return false
    }
  }

  return true
}

func (self *StateMachineFactory) DoExecute(groupIdx int32, instanceId uint64, bodyValue []byte,
                                           smid int32, context *gpaxos.StateMachineContext) bool {
  if smid == 0 {
    return true
  }

  if len(self.StateMachines) == 0 {
    return false
  }

  for _, statemachine := range self.StateMachines {
    if statemachine.SMID() != smid {
      continue
    }

    return statemachine.Execute(groupIdx, instanceId, bodyValue, context)
  }

  log.Error("unknown smid %d instanceid %d", smid, instanceId)

  return false
}

func (self *StateMachineFactory)ExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue[]byte) bool {
  valueLen := len(paxosValue)
  if valueLen < util.INT32SIZE {
    log.Error("value wrong, instance id %d size %d", instanceId, valueLen)
    return true
  }

  var smid int32
  util.DecodeInt32(paxosValue, 0, &smid)

  if smid == 0 {
    log.Error("value no need to do sm,instance id %d", instanceId)
    return true
  }

  bodyValue := paxosValue[util.INT32SIZE:]
  if smid == gpaxos.BATCH_PROPOSE_SMID {
    return self.BatchExecuteForCheckpoint(groupIdx, instanceId, bodyValue)
  } else {
    return self.DoExecuteForCheckpoint(groupIdx, instanceId, bodyValue, smid)
  }
}

func (self *StateMachineFactory)BatchExecuteForCheckpoint(groupIdx int32, instanceId uint64,
                                                          bodyValue []byte) bool {
  var batchPaxosValues common.BatchPaxosValues
  err := proto.Unmarshal(bodyValue, &batchPaxosValues)

  if err != nil {
    log.Error("proto.Unmarshal error %v", err)
    return false
  }

  for _, value := range batchPaxosValues.GetValues() {
    success := self.DoExecuteForCheckpoint(groupIdx, instanceId, value.GetValue(), value.GetSMID())
    if !success {
      return false
    }
  }

  return true
}

func (self *StateMachineFactory)DoExecuteForCheckpoint(groupIdx int32, instanceId uint64,
                                                      bodyValue[]byte, smid int32) bool {
  if smid == 0 {
    return true
  }

  if len(self.StateMachines) == 0 {
    return false
  }

  for _, statemachine := range self.StateMachines {
    if statemachine.SMID() != smid {
      continue
    }

    return statemachine.ExecuteForCheckpoint(groupIdx, instanceId, bodyValue)
  }

  log.Error("unknown smid %d instanceid %d", smid, instanceId)

  return false
}

func (self *StateMachineFactory)PackPaxosValue(value[]byte, smid int32) []byte {
  var paxosValue []byte = make([]byte, util.INT32SIZE)
  util.EncodeInt32(paxosValue, 0, smid)

  return util.AppendBytes(paxosValue, value)
}

func (self *StateMachineFactory) AddStateMachine(stateMachine gpaxos.StateMachine) {
  for _, sm := range(self.StateMachines) {
    if sm.SMID() == stateMachine.SMID() {
      return
    }
  }

  self.StateMachines = append(self.StateMachines, stateMachine)
}

func (self *StateMachineFactory) GetCheckpointInstanceID(groupIdx int32) uint64 {
  cpinstanceId := common.INVALID_INSTANCEID
  cpinstanceId_insize := common.INVALID_INSTANCEID
  haveUseSm := false

  for _, statemachine := range(self.StateMachines) {
    instanceId := statemachine.GetCheckpointInstanceID(groupIdx)
    smid := statemachine.SMID()

    if smid == gpaxos.SYSTEM_V_SMID || smid == gpaxos.MASTER_V_SMID {
      if instanceId == common.INVALID_INSTANCEID {
        continue
      }

      if instanceId > cpinstanceId_insize || cpinstanceId_insize == common.INVALID_INSTANCEID {
        cpinstanceId_insize = instanceId
      }

      continue
    }

    haveUseSm = true
    if instanceId == common.INVALID_INSTANCEID {
      continue
    }

    if instanceId > cpinstanceId || cpinstanceId == common.INVALID_INSTANCEID {
      cpinstanceId = instanceId
    }
  }

  if haveUseSm {
    return cpinstanceId
  }

  return cpinstanceId_insize
}

func (self *StateMachineFactory) GetStateMachines() [] gpaxos.StateMachine{
  return self.StateMachines
}