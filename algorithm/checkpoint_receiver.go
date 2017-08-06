package algorithm

import (
  "github.com/lichuang/gpaxos/config"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/logstorage"
  "github.com/lichuang/gpaxos/util"
  "path/filepath"
  "os"
  "strings"
  "errors"
  "fmt"
  "bytes"
)

type CheckpointReceiver struct {
  config         *config.Config
  logStorage     logstorage.LogStorage
  senderNodeId   uint64
  uuid           uint64
  sequence       uint64
  hasInitDirMaps map[string]bool
}

func NewCheckpointReceiver(config *config.Config, storage logstorage.LogStorage) *CheckpointReceiver {
  receiver := &CheckpointReceiver{
    config:     config,
    logStorage: storage,
  }
  receiver.Reset()
  return receiver
}

func (self *CheckpointReceiver) Reset() {
  self.hasInitDirMaps = make(map[string]bool)
  self.senderNodeId = common.NULL_NODEID
  self.uuid = 0
  self.sequence = 0
}

func (self *CheckpointReceiver) NewReceiver(senderNodeId uint64, uuid uint64) error {
  if !self.ClearCheckpointTmp() {
    return errors.New("clear checkpoint tmp dir error")
  }

  err := self.logStorage.ClearAllLog(self.config.GetMyGroupIdx())
  if err != nil {
    log.Error("ClearAllLog fail, groupidx %d ret %v", self.config.GetMyGroupIdx(), err)
    return err
  }

  self.hasInitDirMaps = make(map[string]bool)
  self.senderNodeId = senderNodeId
  self.uuid = uuid
  self.sequence = 0

  return nil
}

func (self *CheckpointReceiver) ClearCheckpointTmp() bool {
  path := self.logStorage.GetLogStorageDirPath(self.config.GetMyGroupIdx())

  if !util.IsDirectoryExist(path) {
    return false
  }

  filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
    if f == nil {
      return err
    }

    if strings.Contains(path, "cp_tmp_") {
      os.Remove(path)
    }
    return nil
  })

  return true
}

func (self *CheckpointReceiver) IsReceiverFinish(senderNodeId uint64, uuid uint64, endSequence uint64) bool {
  if senderNodeId == self.senderNodeId &&
    uuid == self.uuid && endSequence == self.sequence+1 {
    return true
  }

  return false
}

func (self *CheckpointReceiver) GetTmpDirPath(smid int32) string {
  path := self.logStorage.GetLogStorageDirPath(self.config.GetMyGroupIdx())

  return fmt.Sprintf("%s/cp_tmp_%d", path, smid)
}

func (self *CheckpointReceiver) InitFilePath(path string, formatFilePath *string) error {
  log.Info("start filepath %s", path)

  newFilePath := "/" + path + "/"
  dirList := make([]string, 0)

  dirName := ""
  for i := 0;i < len(newFilePath);i++ {
    if newFilePath[i] == '/' {
      dirList = append(dirList, dirName)
      dirName = ""
    } else {
      dirName += string(newFilePath[i])
    }
  }

  *formatFilePath = "/"
  for i, dir := range (dirList) {
    if i+1 == len(dirList) {
      *formatFilePath += dir
    } else {
      *formatFilePath += dir + "/"
      _, exist := self.hasInitDirMaps[*formatFilePath]
      if !exist {
        err := self.CreateDir(*formatFilePath)
        if err != nil {
          return err
        }

        self.hasInitDirMaps[*formatFilePath] = true
      }
    }
  }

  log.Info("ok, format filepath %s", *formatFilePath)
  return nil
}

func (self *CheckpointReceiver) CreateDir(path string) error {
  if !util.IsDirectoryExist(path) {
    return os.Mkdir(path, os.ModePerm)
  }

  return nil
}

func (self *CheckpointReceiver) ReceiveCheckpoint(msg *common.CheckpointMsg) error {
  if msg.GetNodeID() != self.senderNodeId || msg.GetUUID() != self.uuid {
    log.Error("msg not valid, Msg.SenderNodeID %d Receiver.SenderNodeID %d Msg.uuid %d Receiver.uuid %d",
      msg.GetNodeID(), self.senderNodeId, msg.GetUUID(), self.uuid)

    return errors.New("error msg")
  }

  if self.sequence == msg.GetSequence() {
    return nil
  }

  if msg.GetSequence() != self.sequence+1 {
    log.Error("msg seq wrong, msg.seq %d receiver.seq %d", msg.GetSequence(), self.sequence)
    return errors.New("error msg")
  }

  path := self.GetTmpDirPath(msg.GetSMID()) + "/" + msg.GetFilePath()
  var formatPath string
  err := self.InitFilePath(path, &formatPath)
  if err != nil {
    return err
  }

  file, err := os.OpenFile(formatPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
  if err != nil {
    log.Error("open file fail:%v, file path:%s", err, formatPath)
    return err
  }

  offset, _ := file.Seek(0, os.SEEK_END)
  if uint64(offset) != msg.GetOffset() {
    log.Error("file.offset %d not equal to msg.offset %d", offset, msg.GetOffset())
    file.Close()
    return errors.New("error msg")
  }

  n, _ := file.Write(msg.GetBuffer())
  if n != len(msg.GetBuffer()) {
    log.Error("write fail")
    file.Close()
    return errors.New("error msg")
  }

  self.sequence++
  file.Close()

  log.Info("END ok, write len %d", n)
  return nil
}
