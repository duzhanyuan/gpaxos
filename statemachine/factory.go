package statemachine

import (
  "github.com/lichuang/gpaxos/util"
	"github.com/lichuang/gpaxos"
	log "github.com/lichuang/log4go"
	"errors"
	"github.com/lichuang/gpaxos/common"
)

var InvalidPaxosValue = errors.New("invalid paxos value")
var ZeroSMID = errors.New("zero smid")
var EmptyStateMachine = errors.New("empty statemachines")
var UnknownSMID = errors.New("unknown smid")

type StatemachineFactory struct {
	stateMachines map[int32]gpaxos.StateMachine
}

func NewStatemachineFactory() *StatemachineFactory {
  return &StatemachineFactory{
  	stateMachines: make(map[int32]gpaxos.StateMachine, 0),
	}
}

func isValidPaxosValue(value []byte) bool {
	return true
}

func (self *StatemachineFactory) AddStatemachine(statemachine gpaxos.StateMachine) {
	_, exist := self.stateMachines[statemachine.SMID()]
	if exist {
		return
	}

	self.stateMachines[statemachine.SMID()] = statemachine
}

func (self *StatemachineFactory) Execute(instanceId uint64, paxosValue []byte, ctx *gpaxos.StateMachineContext) error {
	if !isValidPaxosValue(paxosValue) {
		log.Error("value wrong, instanceid %d size %d", instanceId, len(paxosValue))
		return InvalidPaxosValue
	}

	body, smid := self.UnpackPaxosValue(paxosValue)
	if smid == 0 {
		return ZeroSMID
	}

	if smid == gpaxos.BATCH_PROPOSE_SMID {

	} else {
		return self.DoExecute(instanceId, body, smid, ctx)
	}

	return nil
}

func (self *StatemachineFactory) DoExecute(instanceId uint64, body []byte, smid int32, ctx *gpaxos.StateMachineContext) error {
	if len(self.stateMachines) == 0 {
		log.Error("no sm, instanceid %d", instanceId)
		return EmptyStateMachine
	}

	sm, exist := self.stateMachines[smid]
	if !exist {
		log.Error("unknown smid %d instanceid %d", smid, instanceId)
		return UnknownSMID
	}

 	return sm.Execute(instanceId, body, ctx)
}

func (self *StatemachineFactory)PackPaxosValue(value[]byte, smid int32) []byte {
  buf := make([] byte, util.INT32SIZE)
  util.EncodeInt32(buf, 0, smid)

  return util.AppendBytes(buf, value)
}

func (self *StatemachineFactory) UnpackPaxosValue(value []byte) ([]byte, int32) {
  var smid int32
  util.DecodeInt32(value, 0, &smid)
  return value[util.INT32SIZE:], smid
}

func (self *StatemachineFactory) GetCheckpointInstanceID() uint64 {
	cpInstanceId := common.INVALID_INSTANCEID
	cpInstanceId_Insize := common.INVALID_INSTANCEID
	haveUseSm := false

	for smid, sm := range self.stateMachines {
		instanceId := sm.GetCheckpointInstanceID()

		if smid == gpaxos.SYSTEM_V_SMID || smid == gpaxos.MASTER_V_SMID {
			if instanceId == common.INVALID_INSTANCEID {
				continue
			}

			if instanceId > cpInstanceId_Insize || cpInstanceId_Insize == common.INVALID_INSTANCEID {
				cpInstanceId_Insize = instanceId
			}

			continue
		}

		haveUseSm = true

		if instanceId == common.INVALID_INSTANCEID {
			continue
		}

		if instanceId > cpInstanceId || cpInstanceId == common.INVALID_INSTANCEID {
			cpInstanceId = instanceId
		}
	}

	if haveUseSm {
		return cpInstanceId
	}
	return cpInstanceId_Insize
}

func (self *StatemachineFactory) ExecuteForCheckpoint(instanceId uint64, paxosValue[]byte) error {
	if !isValidPaxosValue(paxosValue) {
		log.Error("value wrong, instanceid %d size %d", instanceId, len(paxosValue))
		return InvalidPaxosValue
	}

	body, smid := self.UnpackPaxosValue(paxosValue)
	if smid == 0 {
		return ZeroSMID
	}

	if smid == gpaxos.BATCH_PROPOSE_SMID {

	} else {
		return self.DoExecuteForCheckpoint(instanceId, body, smid)
	}

	return nil
}

func (self *StatemachineFactory) DoExecuteForCheckpoint(instanceId uint64, body []byte, smid int32) error {
	if len(self.stateMachines) == 0 {
		log.Error("no sm, instanceid %d", instanceId)
		return EmptyStateMachine
	}

	sm, exist := self.stateMachines[smid]
	if !exist {
		log.Error("unknown smid %d instanceid %d", smid, instanceId)
		return UnknownSMID
	}

	return sm.ExecuteForCheckpoint(instanceId, body)
}