package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/checkpoint"
  "github.com/lichuang/gpaxos/sm_base"
  "github.com/lichuang/gpaxos/util"
  "github.com/lichuang/gpaxos/log"
  "time"
  "github.com/lichuang/gpaxos"
  "os"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/saul/common/errors"
)

const (
  Checkpoint_ACK_TIMEOUT = 120000
  Checkpoint_ACK_LEAD = 10
)

type CheckpointSender struct {
  sendNodeId            uint64
  config                *config.Config
  Learner               *Learner
  factory               *sm_base.StateMachineFactory
  cpMng                 *checkpoint.CheckpointManager
  isEnd                 bool
  isEnded               bool
  uuid                  uint64
  sequence              uint64
  ackSequence           uint64
  absLastAckTime        uint64
  alreadySendedFileMaps map[string]bool
  tmpBuffers            []byte
}

func NewCheckpointSender(sendNodeId uint64, config *config.Config, learner *Learner,
                         factory *sm_base.StateMachineFactory, cpmng *checkpoint.CheckpointManager) *CheckpointSender {
  return &CheckpointSender{
    sendNodeId: sendNodeId,
    config:     config,
    Learner:    learner,
    factory:    factory,
    cpMng:      cpmng,
    uuid:       (config.GetMyNodeId() ^ learner.GetInstanceId()) + uint64(util.Rand()),
    tmpBuffers: make([] byte, 1048576),
  }
}

func (self *CheckpointSender) Stop() {
  if !self.isEnded {
    self.isEnd = true
  }
}

func (self CheckpointSender) Start() {
  go self.main()
}

func (self *CheckpointSender) main() {
  self.absLastAckTime = util.NowTimeMs()
  needContinue := false
  replayer := self.cpMng.GetReplayer()

  for !replayer.IsPause {
    if self.isEnd {
      self.isEnded = true
      return
    }

    needContinue = true
    replayer.Pause()
    log.Debug("wait replayer paused")
    time.Sleep(time.Microsecond * 100)
  }

  err := self.LockCheckpoint()
  if err == nil {
    self.SendCheckpoint()
    self.UnLockCheckpoint()
  }

  if needContinue {
    replayer.Continue()
  }

  log.Info("Checkpoint.Sender [END]")
  self.isEnded = true
}

func (self *CheckpointSender) LockCheckpoint() error {
  stateMachines := self.factory.GetStateMachines()

  lockStateMachines := make([]gpaxos.StateMachine, 0)
  var err error
  for _, statemachine := range  stateMachines {
    err = statemachine.LockCheckpointState()
    if err != nil {
      break
    }

    lockStateMachines = append(lockStateMachines, statemachine)
  }

  if err != nil {
    for _, statemachine := range lockStateMachines {
      statemachine.UnLockCheckpointState()
    }
  }

  return err
}

func (self *CheckpointSender) UnLockCheckpoint() {
  for _, statemachine := range self.factory.GetStateMachines() {
    statemachine.UnLockCheckpointState()
  }
}

func (self *CheckpointSender) SendCheckpoint() {
  err := self.Learner.SendCheckpointBegin(self.sendNodeId, self.uuid, self.sequence,
    self.factory.GetCheckpointInstanceID(self.config.GetMyGroupIdx()))

  if err != nil {
    log.Error("SendCheckpointBegin fail: %v", err)
    return
  }

  self.sequence++

  for _, statemachine := range self.factory.GetStateMachines() {
    err := self.SendCheckpointForStatemachine(statemachine)
    if err != nil {
      return
    }
  }

  err = self.Learner.SendCheckpointEnd(self.sendNodeId, self.uuid, self.sequence,
    self.factory.GetCheckpointInstanceID(self.config.GetMyGroupIdx()))
  if err != nil {
    log.Error("SendCheckpointEnd fail %v, sequence %d", err, self.sequence)
  }
}

func (self *CheckpointSender) SendCheckpointForStatemachine(statemachine gpaxos.StateMachine) error {
  var dirPath string
  fileList := make([]string, 0)

  err := statemachine.GetCheckpointState(self.config.GetMyGroupIdx(), &dirPath, fileList)
  if err != nil {
    log.Error("GetCheckpointState fail %v, smid %d", err, statemachine.SMID())
    return err
  }

  if len(dirPath) == 0 {
    log.Info("No Checkpoint, smid %d", statemachine.SMID())
    return nil
  }

  if dirPath[len(dirPath)-1] != '/' {
    dirPath += "/"
  }

  for _, file := range fileList {
    err := self.SendFile(statemachine, dirPath, file)
    if err != nil {
      log.Error("SendFile fail %v smid %d", err, statemachine.SMID())
      return err
    }
  }

  log.Info("END, send ok, smid %d filelistcount %d", statemachine.SMID(), len(fileList))
  return nil
}

func (self *CheckpointSender) SendFile(stateMachine gpaxos.StateMachine, dirPath string, filePath string) error {
  log.Info("START smid %d dirpath %s filepath %s", stateMachine.SMID(), dirPath, filePath)
  path := dirPath + filePath

  _, exist := self.alreadySendedFileMaps[path]
  if exist {
    log.Error("file already send, filepath %s", path)
    return nil
  }

  file, err := os.Open(path)
  if err != nil {
    log.Error("Open file fail %v filepath %s", err, path)
    return err
  }

  var readLen int = 0
  var offset uint64 = 0
  for {
    readLen, err = file.Read(self.tmpBuffers)
    if err != nil {
      file.Close()
      return err
    }

    if readLen == 0 {
      break
    }

    instanceId := stateMachine.GetCheckpointInstanceID(self.config.GetMyGroupIdx())
    err = self.SendBuffer(stateMachine.SMID(), instanceId,
      filePath, offset, self.tmpBuffers[:readLen])
    if err != nil {
      file.Close()
      return err
    }

    log.Debug("Send ok, offset %d readlen %d", offset, readLen)
    if readLen < len(self.tmpBuffers) {
      break
    }

    offset += uint64(readLen)
  }

  self.alreadySendedFileMaps[path] = true

  file.Close()
  log.Info("end")
  return nil
}

func (self *CheckpointSender) SendBuffer(smid int32, checkpointInstanceId uint64, filePath string, offset uint64, buffer[]byte) error {
  cksum := util.Crc32(0, buffer, common.CRC32_SKIP)

  var err error
  for {
    if self.isEnd {
      return errors.New("isend")
    }

    if !self.CheckAck(self.sequence) {
      return errors.New("checkack fail")
    }

    err = self.Learner.SendCheckpoint(self.sendNodeId, self.uuid, self.sequence, checkpointInstanceId,
      cksum, filePath, smid, offset, buffer)
    if err == nil {
      self.sequence++
      break
    } else {
      log.Error("SendCheckpoint fail %v need sleep 30s", err)
      time.Sleep(time.Second * 30)
    }
  }

  return err
}

func (self *CheckpointSender) Ack(sendNodeId uint64, uuid uint64, sequence uint64) {
  if sendNodeId != self.sendNodeId {
    log.Error("send nodeid not same, ack.sendnodeid %d self.sendnodeid %d", sendNodeId, self.sendNodeId)
    return
  }

  if self.uuid != uuid {
    log.Error("uuid not same, ack.uuid %d self.uuid %d", uuid, self.uuid)
    return
  }

  if sequence != self.sequence {
    log.Error("ack_sequence not same, ack.ack_sequence %d self.ack_sequence %d", sequence, self.sequence)
    return
  }

  self.ackSequence++
  self.absLastAckTime = util.NowTimeMs()
}

func (self *CheckpointSender) CheckAck(sendSequence uint64) bool {
  for sendSequence < self.ackSequence + Checkpoint_ACK_LEAD {
    nowTime := util.NowTimeMs()
    var passTime uint64 = 0
    if nowTime > self.absLastAckTime {
      passTime = nowTime - self.absLastAckTime
    }

    if self.isEnd {
      return false
    }

    if passTime >= Checkpoint_ACK_TIMEOUT {
      log.Error("Ack timeout, last acktime %d", self.absLastAckTime)
      return false
    }

    time.Sleep(time.Microsecond * 20)
  }

  return true
}

func (self *CheckpointSender) End() {
  self.isEnd = true
}

func (self *CheckpointSender) IsEnd() bool {
  return self.isEnd
}