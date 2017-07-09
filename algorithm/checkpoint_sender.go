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
  SendNodeId           uint64
  Config               *config.Config
  Learner              *Learner
  Factory              *sm_base.StateMachineFactory
  CpMng                *checkpoint.CheckpointManager
  isEnd                bool
  IsEnded              bool
  UUID                 uint64
  Sequence             uint64
  AckSequence          uint64
  AbsLastAckTime       uint64
  AlreadySendedFileMap map[string]bool
  TmpBuffer []byte
}

func NewCheckpointSender(sendNodeId uint64, config *config.Config, learner *Learner,
                         factory *sm_base.StateMachineFactory, cpmng *checkpoint.CheckpointManager) *CheckpointSender {
  return &CheckpointSender{
    SendNodeId:sendNodeId,
    Config:config,
    Learner:learner,
    Factory:factory,
    CpMng:cpmng,
    UUID:(config.GetMyNodeId() ^ learner.GetInstanceId()) + uint64(util.Rand()),
    TmpBuffer:make([] byte, 1048576),
  }
}

func (self *CheckpointSender) Stop() {
  if !self.IsEnded {
    self.isEnd = true
  }
}

func (self CheckpointSender) Start() {
  go self.main()
}

func (self *CheckpointSender) main() {
  self.AbsLastAckTime = util.NowTimeMs()
  needContinue := false

  for !self.CpMng.replayer.IsPause {
    if self.isEnd {
      self.IsEnded = true
      return
    }

    needContinue = true
    self.CpMng.replayer.Pause()
    log.Debug("wait replayer paused")
    time.Sleep(time.Microsecond * 100)
  }

  err := self.LockCheckpoint()
  if err == nil {
    self.SendCheckpoint()
    self.UnLockCheckpoint()
  }

  if needContinue {
    self.CpMng.replayer.Continue()
  }

  log.Info("Checkpoint.Sender [END]")
  self.IsEnded = true
}

func (self *CheckpointSender) LockCheckpoint() error {
  stateMachines := self.Factory.StateMachines

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
  for _, statemachine := range self.Factory.StateMachines {
    statemachine.UnLockCheckpointState()
  }
}

func (self *CheckpointSender) SendCheckpoint() {
  err := self.Learner.SendCheckpointBegin(self.SendNodeId, self.UUID, self.Sequence,
    self.Factory.GetCheckpointInstanceID(self.Config.GetMyGroupIdx()))

  if err != nil {
    log.Error("SendCheckpointBegin fail: %v", err)
    return
  }

  self.Sequence++

  for _, statemachine := range self.Factory.StateMachines {
    err := self.SendCheckpointFofaSM(statemachine)
    if err != nil {
      return
    }
  }

  err = self.Learner.SendCheckpointEnd(self.SendNodeId, self.UUID, self.Sequence,
    self.Factory.GetCheckpointInstanceID(self.Config.GetMyGroupIdx()))
  if err != nil {
    log.Error("SendCheckpointEnd fail %v, sequence %d", err, self.Sequence)
  }
}

func (self *CheckpointSender) SendCheckpointFofaSM(statemachine gpaxos.StateMachine) error {
  var dirPath string
  fileList := make([]string, 0)

  err := statemachine.GetCheckpointState(self.Config.GetMyGroupIdx(), &dirPath, fileList)
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

  _, exist := self.AlreadySendedFileMap[path]
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
    readLen, err = file.Read(self.TmpBuffer)
    if err != nil {
      file.Close()
      return err
    }

    if readLen == 0 {
      break
    }

    err = self.SendBuffer(stateMachine.SMID(), stateMachine.GetCheckpointInstanceID(self.Config.GetMyGroupIdx()),
      filePath, offset, self.TmpBuffer[:readLen])
    if err != nil {
      file.Close()
      return err
    }

    log.Debug("Send ok, offset %d readlen %d", offset, readLen)
    if readLen < len(self.TmpBuffer) {
      break
    }

    offset += uint64(readLen)
  }

  self.AlreadySendedFileMap[path] = true

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

    if !self.CheckAck(self.Sequence) {
      return errors.New("checkack fail")
    }

    err = self.Learner.SendCheckpoint(self.SendNodeId, self.UUID, self.Sequence, checkpointInstanceId,
      cksum, filePath, smid, offset, buffer)
    if err == nil {
      self.Sequence++
      break
    } else {
      log.Error("SendCheckpoint fail %v need sleep 30s", err)
      time.Sleep(time.Second * 30)
    }
  }

  return err
}

func (self *CheckpointSender) Ack(sendNodeId uint64, uuid uint64, sequence uint64) {
  if sendNodeId != self.SendNodeId {
    log.Error("send nodeid not same, ack.sendnodeid %d self.sendnodeid %d", sendNodeId, self.SendNodeId)
    return
  }

  if self.UUID != uuid {
    log.Error("uuid not same, ack.uuid %d self.uuid %d", uuid, self.UUID)
    return
  }

  if sequence != self.Sequence {
    log.Error("ack_sequence not same, ack.ack_sequence %d self.ack_sequence %d", sequence, self.Sequence)
    return
  }

  self.AckSequence++
  self.AbsLastAckTime = util.NowTimeMs()
}

func (self *CheckpointSender) CheckAck(sendSequence uint64) bool {
  for sendSequence < self.AckSequence + Checkpoint_ACK_LEAD {
    nowTime := util.NowTimeMs()
    var passTime uint64 = 0
    if nowTime > self.AbsLastAckTime {
      passTime = nowTime - self.AbsLastAckTime
    }

    if self.isEnd {
      return false
    }

    if passTime >= Checkpoint_ACK_TIMEOUT {
      log.Error("Ack timeout, last acktime %d", self.AbsLastAckTime)
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