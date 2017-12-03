package algorithm

import (
  "github.com/golang/protobuf/proto"

  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/storage"

  log "github.com/lichuang/log4go"
)

type LearnerState struct {
  config       *config.Config
  learnedValue []byte
  isLearned    bool
  newChecksum  uint32
  paxosLog     *storage.PaxosLog
}

func NewLearnerState(instance *Instance) *LearnerState {
  state := &LearnerState{
    config:      instance.config,
    paxosLog:    instance.paxosLog,
  }

  state.Init()

  return state
}

func (self *LearnerState) GetNewChecksum() uint32 {
  return self.newChecksum
}

func (self *LearnerState) LearnValueWithoutWrite(instanceId uint64, value []byte, checksum uint32) {
  self.learnedValue = value
  self.isLearned = true
  self.newChecksum = checksum
}

func (self *LearnerState) LearnValue(instanceId uint64, learnedBallot BallotNumber, value []byte, lastChecksum uint32) error {
  if instanceId == 0 && lastChecksum == 0 {
    self.newChecksum = 0
  } else if len(value) > 0 {
    self.newChecksum = util.Crc32(lastChecksum, value, common.CRC32_SKIP)
  }

  state := common.AcceptorStateData{
    InstanceID:     proto.Uint64(instanceId),
    AcceptedValue:  proto.NewBuffer(value).Bytes(),
    PromiseID:      proto.Uint64(learnedBallot.proposalId),
    PromiseNodeID:  proto.Uint64(learnedBallot.nodeId),
    AcceptedID:     proto.Uint64(learnedBallot.nodeId),
    AcceptedNodeID: proto.Uint64(learnedBallot.nodeId),
    Checksum:       proto.Uint32(self.newChecksum),
  }

  options := storage.WriteOptions{
    Sync: false,
  }

  err := self.paxosLog.WriteState(options, instanceId, state)
  if err != nil {
    log.Error("storage writestate fail, instanceid %d valuelen %d err %v",
      instanceId, len(value), err)
    return err
  }

  self.LearnValueWithoutWrite(instanceId, value, self.newChecksum)

  return nil
}

func (self *LearnerState) GetLearnValue() []byte {
  return self.learnedValue
}

func (self *LearnerState) IsLearned() bool {
  return self.isLearned
}

func (self *LearnerState) Init() {
  self.learnedValue = make([]byte, 0)
  self.isLearned = false
  self.newChecksum = 0
}
