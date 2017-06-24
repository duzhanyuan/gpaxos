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
  Config *config.Config
  Logstorage logstorage.LogStorage
  SenderNodeId uint64
  UUID uint64
  Sequence uint64
  HasInitDirMap map[string]bool
}

func NewCheckpointReceiver(config *config.Config, storage *logstorage.LogStorage) *CheckpointReceiver {
  receiver := &CheckpointReceiver{
    Config:config,
    Logstorage:storage,
  }
  receiver.Reset()
  return receiver
}

func (self *CheckpointReceiver) Reset() {
  self.HasInitDirMap = make(map[string]bool)
  self.SenderNodeId = common.NULL_NODEID
  self.UUID = 0
  self.Sequence = 0
}

func (self *CheckpointReceiver) NewReceiver(senderNodeId uint64, uuid uint64) error {
  if !self.ClearCheckpointTmp() {
    return errors.New("clear checkpoint tmp dir error")
  }

  err := self.Logstorage.ClearAllLog(self.Config.GetMyGroupIdx())
  if err != nil {
    return err
  }

  self.HasInitDirMap = make(map[string]bool)
  self.SenderNodeId = senderNodeId
  self.UUID = uuid
  self.Sequence = 0

  return nil
}

func (self *CheckpointReceiver)ClearCheckpointTmp() bool {
  path := self.Logstorage.GetLogStorageDirPath(self.Config.GetMyGroupIdx())

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

func (self *CheckpointReceiver)IsReceiverFinish(senderNodeId uint64, uuid uint64, endSequence uint64) bool{
  if senderNodeId == self.SenderNodeId &&
     uuid == self.UUID && endSequence == self.Sequence + 1 {
    return true
  }

  return false
}

func (self *CheckpointReceiver)GetTmpDirPath(smid int32) string {
  path := self.Logstorage.GetLogStorageDirPath(self.Config.GetMyGroupIdx())

  return fmt.Sprintf("%s/cp_tmp_%d", path, smid)
}

func (self *CheckpointReceiver) InitFilePath(path string, formatFilePath *string) error {
  log.Info("start filepath %s", path)

  newFilePath := "/" + path + "/"
  dirList := make([]string, 0)

  dirName := bytes.NewBufferString("")
  i := 0
  for i < len(newFilePath) {
    if newFilePath[i] == '/' {
      dirList = append(dirList, dirName.String())
      dirName = bytes.NewBufferString("")
    } else {
      dirName.WriteByte(newFilePath[i])
    }
  }

  *formatFilePath = ""
  for i, dir := range(dirList) {
    if i + 1 == len(dirList) {
      *formatFilePath += dir
    } else {
      *formatFilePath += dir + "/"
      _, exist := self.HasInitDirMap[*formatFilePath]
      if !exist {
        err := self.CreateDir(*formatFilePath)
        if err != nil {
          return err
        }

        self.HasInitDirMap[*formatFilePath] = true
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

func (self *CheckpointReceiver) ReceiveCheckpoint(msg common.CheckpointMsg) error {
  if msg.GetNodeID() != self.SenderNodeId || msg.GetUUID() != self.UUID {
    log.Error("msg not valid, Msg.SenderNodeID %d Receiver.SenderNodeID %d Msg.UUID %d Receiver.UUID %d",
      msg.GetNodeID(), self.SenderNodeId, msg.GetUUID(), self.UUID)

    return errors.New("error msg")
  }

  if self.Sequence == msg.GetSequence() {
    return nil
  }

  if msg.GetSequence() != self.Sequence + 1 {
    log.Error("msg seq wrong, msg.seq %d receiver.seq %d", msg.GetSequence(), self.Sequence)
    return errors.New("error msg")
  }

  path := self.GetTmpDirPath(msg.GetSMID()) + "/" + msg.GetFilePath()
  var formatPath string
  err := self.InitFilePath(path, &formatPath)
  if err != nil {
    return err
  }

  file, err := os.OpenFile(formatPath, os.O_CREATE | os.O_RDWR | os.O_APPEND, os.ModePerm)
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

  self.Sequence++
  file.Close()

  log.Info("END ok, write len %d", n)
  return nil
}