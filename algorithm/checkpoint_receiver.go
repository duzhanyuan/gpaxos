package algorithm

import (
	"github.com/lichuang/gpaxos/config"
	"github.com/lichuang/gpaxos/storage"
	"github.com/lichuang/gpaxos/common"
	"os"
	"io/ioutil"
	"strings"
	"fmt"
	log "github.com/lichuang/log4go"
)

type CheckpointReceiver struct {
	config *config.Config
	logStorage *storage.LogStorage
	senderNodeId uint64
	uuid uint64
	sequence uint64
	hasInitDirMap map[string]bool
}

func NewCheckpointReceiver(config *config.Config, logStorage *storage.LogStorage) *CheckpointReceiver {
	ckRver := &CheckpointReceiver{
		config: config,
		logStorage:logStorage,
	}

	ckRver.Reset()

	return ckRver
}

func (self *CheckpointReceiver) Reset() {
	self.hasInitDirMap = make(map[string]bool, 0)
	self.senderNodeId = common.NULL_NODEID
	self.uuid = 0
	self.sequence = 0
}

func (self *CheckpointReceiver) NewReceiver(senderNodeId uint64, uuid uint64) error {
	err := self.ClearCheckpointTmp()
	if err != nil {
		return err
	}

	err = self.logStorage.ClearAllLog()
	if err != nil {
		return err
	}

	self.hasInitDirMap = make(map[string]bool, 0)
	self.senderNodeId = senderNodeId
	self.uuid = uuid
	self.sequence = 0

	return nil
}

func (self *CheckpointReceiver) ClearCheckpointTmp() error {
	logStoragePath := self.logStorage.GetLogStorageDirPath()
	files, err := ioutil.ReadDir(logStoragePath)

	for _, file := range files {
		if strings.Contains(file.Name(), "cp_tmp_") {
			err = os.Remove(logStoragePath + "/" + file.Name())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *CheckpointReceiver)IsReceiverFinish(senderNodeId uint64, uuid uint64, endSequence uint64) bool {
	if senderNodeId != self.senderNodeId {
		return false
	}

	if uuid != self.uuid {
		return false
	}

	if endSequence != self.sequence {
		return false
	}

	return true
}

func (self *CheckpointReceiver)GetTmpDirPath(smid int32) string {
	logStoragePath := self.logStorage.GetLogStorageDirPath()
	return fmt.Sprintf("$s/cp_tmp_%d", logStoragePath, smid)
}

func (self *CheckpointReceiver)InitFilePath(filePath string) (string, error){
	newFilePath := "/" + filePath + "/"
	dirList := make([]string, 0)

	dirName := ""
	for i := 0; i < len(newFilePath); i++ {
		if newFilePath[i] == '/' {
			if len(dirName) > 0 {
				dirList = append(dirList, dirName)
			}

			dirName = ""
		} else {
			dirName += fmt.Sprintf("%c", newFilePath[i])
		}
	}

	formatFilePath := "/"
	for i, dir := range dirList {
		if i + 1 == len(dirList)	 {
			formatFilePath += dir
		} else {
			formatFilePath += dir + "/"
			_, exist := self.hasInitDirMap[formatFilePath]
			if !exist {
				err := self.CreateDir(formatFilePath)
				if err != nil {
					return "", err
				}

				self.hasInitDirMap[formatFilePath] = true
			}
		}
	}

	log.Debug("ok, format filepath %s", formatFilePath)
	return formatFilePath, nil
}

func (self *CheckpointReceiver) CreateDir(dirPath string) error {
	_, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		return os.Mkdir(dirPath, os.ModeDir)
	}

	return nil
}

func (self *CheckpointReceiver) ReceiveCheckpoint(ckMsg *common.CheckpointMsg) error {
	if ckMsg.GetNodeID() != self.senderNodeId || ckMsg.GetUUID() != self.uuid {
		return common.ErrInvalidMsg
	}

	if ckMsg.GetSequence() == self.sequence {
		log.Error("msg already received, msg sequence %d receiver sequence %d", ckMsg.GetSequence(), self.sequence)
		return nil
	}

	if ckMsg.GetSequence() != self.sequence + 1 {
		log.Error("msg sequence wrong, msg sequence %d receiver sequence %d", ckMsg.GetSequence(), self.sequence)
		return common.ErrInvalidMsg
	}

	filePath := self.GetTmpDirPath(ckMsg.GetSMID()) + "/" + ckMsg.GetFilePath()
	formatFilePath, err := self.InitFilePath(filePath)
	if err != nil {
		return err
	}

	file, err := os.Open(formatFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil	{
		return err
	}

	if uint64(offset) != ckMsg.GetOffset() {
		log.Error("wrong msg, file offset %d msg offset %d", offset, ckMsg.GetOffset())
		return common.ErrInvalidMsg
	}

	writeLen, err := file.Write(ckMsg.GetBuffer())
	if err != nil || writeLen != len(ckMsg.GetBuffer()) {
		log.Error("write fail, write len %d", writeLen)
		return common.ErrWriteFileFail
	}

	self.sequence+=1
	log.Debug("end ok, write len %d", writeLen)
	return nil
}