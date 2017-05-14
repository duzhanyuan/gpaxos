package logstorage

import (
	"bytes"
	"os"

	"strconv"

	"fmt"

	"syscall"

	proto "github.com/golang/protobuf/proto"

	"github.com/lichuang/gpaxos/common"
	"github.com/lichuang/gpaxos/log"
	"github.com/lichuang/gpaxos/util"
)

type fileLogger struct {
}

func (self *fileLogger) Init(path string) {

}

func (self *fileLogger) Log(fmt string, args ...interface{}) {
}

type LogStore struct {
	MyGroupIdx    int
	Path          string
	FileLogger    fileLogger
	MetaFile      *os.File
	FileId        int32
	NowFileOffset uint64
	File          *os.File
}

func (self *LogStore) Init(path string, groupIdx int, db *Database) error {
	self.MyGroupIdx = groupIdx
	self.Path = path + "/vfile"
	err := syscall.Access(self.Path, syscall.F_OK)
	if err != nil {
		err := os.Mkdir(self.Path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("create dir %s error: %v", self.Path, err)
		}
	}

	self.FileLogger.Init(self.Path)

	var metaFilePath = self.Path + "/meta"
	metaFile, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return fmt.Errorf("open meta file %s error: %v", metaFilePath, err)
	}
	self.MetaFile = metaFile

	_, err = metaFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("seek file %s error %v", metaFile.Name(), err)
	}

	buff := make([]byte, util.INT32SIZE)
	n, err := metaFile.Read(buff)
	if n != util.INT32SIZE {
		if n == 0 { // no content
			self.FileId = 0
		} else {
			return fmt.Errorf("read meta file %s info fail, real len %d", metaFile.Name(), n)
		}
	}
	fileIdstr := string(buff)
	fileId, _ := strconv.Atoi(fileIdstr)
	self.FileId = int32(fileId)

	var metaCkSum uint32
	buff = make([]byte, util.UINT32SIZE)
	n, err = metaFile.Read(buff)
	if n == util.UINT32SIZE {
		sum, _ := strconv.Atoi(string(buff))
		metaCkSum = uint32(sum)
		ckSum := util.Crc32(0, []byte(fileIdstr), 1)
		if metaCkSum != ckSum {
			return fmt.Errorf("meta file checksum %d not same to cal checksum %d, file id %s",
				metaCkSum, ckSum, fileIdstr)
		}
	}

	err = self.RebuildIndex(db, &self.NowFileOffset)
	if err != nil {
		return err
	}

	self.File, err = self.OpenFile(self.FileId)
	if err != nil {
		return err
	}

	nowFileOffset, err := self.File.Seek(int64(self.NowFileOffset), os.SEEK_SET)
	if err != nil {
		return err
	}
	self.NowFileOffset = uint64(nowFileOffset)

	return nil
}

func (self *LogStore) RebuildIndex(db *Database, nowOffset *uint64) error {
	var lastFileId string
	var nowInstanceId uint64

	err := db.GetMaxInstanceIDFileID(&lastFileId, &nowInstanceId)
	if err != nil {
		return err
	}

	var fileId int32
	var offset uint64
	var cksum uint32
	if len(lastFileId) > 0 {
		self.DecodeFileId(lastFileId, &fileId, &offset, &cksum)
	}

	if fileId > self.FileId {
		return fmt.Errorf("leveldb last fileid %d lagger than meta now fileid %d", fileId, self.FileId)
	}

	log.Info("start fileid %d offset %d checksum %d", fileId, offset, cksum)

	nowFileId := fileId
	for {
		err = self.RebuildIndexForOneFile(nowFileId, offset, db, nowOffset, &nowInstanceId)
		if err != nil {
			break
		}

		if nowFileId != 0 && nowFileId != self.FileId+1 {
			err = fmt.Errorf("meta file wrong, now file id %d meta file id %d", nowFileId, self.FileId)
			log.Error("%v", err)
			return err
		}
		err = nil
		log.Info("end rebuild ok, now file id:%d", nowFileId)

		offset = 0

		nowFileId++
	}
	return nil
}

func (self *LogStore) RebuildIndexForOneFile(fileId int32, offset uint64, db *Database, nowWriteOffset *uint64, nowInstanceId *uint64) error {
	var err error = nil

	var file *os.File = nil
	defer func() {
		if file != nil {
			file.Close()
		}
	}()
	filePath := fmt.Sprintf("%s/%d.f", self.Path, fileId)

	err = syscall.Access(filePath, syscall.F_OK)
	if err != nil {
		return err
	}

	file, err = self.OpenFile(fileId)
	if err != nil {
		return err
	}

	fileLen, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		file.Close()
		return err
	}

	_, err = file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		file.Close()
		return err
	}

	var nowOffset = offset
	var needTruncate = false

	for {
		buffer := make([]byte, util.INT32SIZE)
		n, err := file.Read(buffer)
		if n == 0 {
			*nowWriteOffset = nowOffset
			log.Debug("file end, file id %d offset %d", fileId, nowOffset)
			err = nil
			break
		}

		if n != util.INT32SIZE {
			needTruncate = true
			log.Error("read len %d not equal to %d, need truncate", n, util.INT32SIZE)
			err = nil
			break
		}

		len, _ := strconv.Atoi(string(buffer))
		if len == 0 {
			*nowWriteOffset = nowOffset
			log.Debug("file end, file id %d offset %d", fileId, nowOffset)
			break
		}

		if int64(len) > fileLen || len < util.UINT64SIZE {
			err = fmt.Errorf("file data len wrong, data len %d filelen %d", len, fileLen)
			log.Error("%v", err)
			break
		}

		buffer = make([]byte, len)
		n, err = file.Read(buffer)
		if n != len {
			needTruncate = true
			log.Error("read len %d not equal to %d, need truncate", n, len)
			break
		}

		var instanceId uint64
		util.DecodeUint64(buffer, 0, &instanceId)

		if instanceId < *nowInstanceId {
			log.Error("file data wrong,read instanceid %d smaller than now instanceid %d", instanceId, *nowInstanceId)
			break
		}

		*nowInstanceId = instanceId

		var state common.AcceptorStateData
		err = proto.Unmarshal(buffer[util.UINT64SIZE:], &state)
		if err != nil {
			self.NowFileOffset = uint64(nowOffset)
			needTruncate = true
			log.Error("this instance buffer wrong, can't parse to acceptState, instanceid %d bufferlen %d nowoffset %d",
				instanceId, len-util.UINT64SIZE, nowOffset)
			break
		}

		fileCkSum := util.Crc32(0, buffer, common.CRC32_SKIP)
		var fileIdstr string
		self.EncodeFileId(fileId, nowOffset, fileCkSum, &fileIdstr)

		err = db.RebuildOneIndex(instanceId, fileIdstr)
		if err != nil {
			break
		}

		log.Info("rebuild one index ok, fileid %d offset %d instanceid %d cksum %d buffer size %d",
			fileId, nowOffset, instanceId, fileCkSum, len-util.UINT64SIZE)

		nowOffset += uint64(util.INT32SIZE) + uint64(len)
	}

	if needTruncate {
		self.FileLogger.Log("truncate fileid %d offset %d filesize %d", fileId, nowOffset, fileLen)
		err = os.Truncate(filePath, int64(nowOffset))
		if err != nil {
			log.Error("truncate fail, file path %s truncate to length %d error:%v",
				filePath, nowOffset, err)
		}
	}
	return err
}

func (self *LogStore) OpenFile(fileId int32) (*os.File, error) {
	filePath := fmt.Sprintf("%s/%d.f", self.Path, fileId)
	return os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func (self *LogStore) EncodeFileId(fileId int32, offset uint64, cksum uint32, fileIdStr *string) {
	buffer := make([]byte, util.INT32SIZE+util.UINT64SIZE+util.UINT32SIZE)
	util.EncodeInt32(buffer, 0, fileId)
	util.EncodeUint64(buffer, util.INT32SIZE, offset)
	util.EncodeUint32(buffer, util.INT32SIZE+util.UINT32SIZE, cksum)

	*fileIdStr = string(buffer)
}

func (self *LogStore) DecodeFileId(fileIdStr string, fileId *int32, offset *uint64, cksum *uint32) {
	buffer := bytes.NewBufferString(fileIdStr).Bytes()

	util.DecodeInt32(buffer, 0, fileId)
	util.DecodeUint64(buffer, util.INT32SIZE, offset)
	util.DecodeUint32(buffer, util.INT32SIZE+util.UINT64SIZE, cksum)
}
