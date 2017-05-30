package logstorage

import (
	"bytes"
	"os"

	"strconv"

	"fmt"

	"syscall"

	proto "github.com/golang/protobuf/proto"

	"sync"

	"time"

	"github.com/lichuang/gpaxos/common"
	"github.com/lichuang/gpaxos/log"
	"github.com/lichuang/gpaxos/util"
)

type fileLogger struct {
	File *os.File
}

func (self *fileLogger) Init(path string) {
	filePath := fmt.Sprintf("%s/LOG", path)
	self.File, _ = os.Open(filePath)
}

func (self *fileLogger) Log(format string, args ...interface{}) {
	if self.File == nil {
		return
	}

	nowTimeMs := time.Now().UnixNano() / 1000000
	timePrefix := time.Now().Format("2006-01-02 15:04:05")

	prefix := fmt.Sprintf("%s:%d", timePrefix, nowTimeMs%1000)
	newFormat := prefix + format + "\n"

	buf := fmt.Sprintf(newFormat, args)
	self.File.Write([]byte(buf))
}

type LogStore struct {
	MyGroupIdx       int32
	Path             string
	FileLogger       fileLogger
	MetaFile         *os.File
	FileId           int32
	NowFileOffset    uint64
	NowFileSize      uint64
	File             *os.File
	ReadMutex        sync.Mutex
	Mutex            sync.Mutex
	DeletedMaxFileId int32
}

func (self *LogStore) Init(path string, groupIdx int32, db *Database) error {
	self.DeletedMaxFileId = -1
	self.MyGroupIdx = groupIdx
	self.Path = path + "/vfile"
	self.File = nil

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
	util.DecodeInt32(buff, 0, &self.FileId)
	ckSum := util.Crc32(0, buff, 0)

	var metaCkSum uint32
	buff = make([]byte, util.UINT32SIZE)
	n, err = metaFile.Read(buff)
	if n == util.UINT32SIZE {
		util.DecodeUint32(buff, 0, &metaCkSum)
		if metaCkSum != ckSum {
			return fmt.Errorf("meta file checksum %d not same to cal checksum %d, file id %d",
				metaCkSum, ckSum, self.FileId)
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

	err = self.ExpendFile(self.File, &self.NowFileSize)
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

func (self *LogStore) ExpendFile(file *os.File, fileSize *uint64) error {
	var err error
	var size int64
	size, err = file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	*fileSize = uint64(size)

	if *fileSize == 0 {
		maxLogFileSize := common.GetLogFileMaxSize()
		size, err = file.Seek(maxLogFileSize-1, os.SEEK_SET)
		*fileSize = uint64(size)
		if err != nil {
			return err
		}

		file.Write([]byte{0})

		*fileSize = uint64(maxLogFileSize)
		file.Seek(0, os.SEEK_SET)
		self.NowFileOffset = 0
	}

	return nil
}

func (self *LogStore) IncreaseFileId() error {
	fileId := self.FileId + 1
	buffer := make([]byte, util.INT32SIZE)
	util.EncodeInt32(buffer, 0, fileId)

	_, err := self.MetaFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	n, err := self.MetaFile.Write(buffer)
	if err != nil {
		return err
	}
	if n != util.INT32SIZE {
		return fmt.Errorf("write len %d not equal to %d", n, util.INT32SIZE)
	}
	ckSum := util.Crc32(0, buffer, 0)
	buffer = make([]byte, util.UINT32SIZE)
	util.EncodeUint32(buffer, 0, ckSum)
	n, err = self.MetaFile.Write(buffer)
	if err != nil {
		return err
	}

	if n != util.UINT32SIZE {
		return fmt.Errorf("write len %d not equal to %d", n, util.UINT32SIZE)
	}

	self.MetaFile.Sync()

	self.FileId += 1
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

func (self *LogStore) Read(fileIdstr string, instanceId *uint64, buffer *string) error {
	var fileId int32
	var offset uint64
	var cksum uint32
	self.DecodeFileId(fileIdstr, &fileId, &offset, &cksum)

	file, err := self.OpenFile(fileId)
	if err != nil {
		return err
	}

	_, err = file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return err
	}

	tmpbuf := make([]byte, util.INT32SIZE)
	n, err := file.Read(tmpbuf)
	if err != nil {
		return err
	}
	if n != util.INT32SIZE {
		return fmt.Errorf("read len %d not equal to %d", n, util.INT32SIZE)
	}

	bufferlen, err := strconv.Atoi(string(tmpbuf))
	if err != nil {
		return err
	}

	self.ReadMutex.Lock()
	defer self.ReadMutex.Unlock()

	tmpbuf = make([]byte, bufferlen)
	n, err = file.Read(tmpbuf)
	if err != nil {
		return err
	}

	if n != bufferlen {
		return fmt.Errorf("read len %d not equal to %d", n, bufferlen)
	}

	fileCkSum := util.Crc32(0, tmpbuf, common.CRC32_SKIP)
	if fileCkSum != cksum {
		return fmt.Errorf("cksum not equal, file cksum %d, cksum %d", fileCkSum, cksum)
	}

	util.DecodeUint64(tmpbuf, 0, instanceId)

	*buffer = string(tmpbuf[util.UINT64SIZE:])

	log.Info("ok, fileid %d offset %d instanceid %d buffser size %d",
		fileId, offset, *instanceId, bufferlen-util.UINT64SIZE)

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

func (self *LogStore) DeleteFile(fileId int32) error {
	if self.DeletedMaxFileId == -1 {
		if fileId-2000 > 0 {
			self.DeletedMaxFileId = fileId - 2000
		}
	}

	if fileId <= self.DeletedMaxFileId {
		log.Debug("file already deleted, fileid %d deletedmaxfileid %d", fileId, self.DeletedMaxFileId)
		return nil
	}

	var err error
	for deleteFileId := self.DeletedMaxFileId + 1; deleteFileId <= fileId; deleteFileId++ {
		filePath := fmt.Sprintf("%s/%d.f", self.Path, deleteFileId)

		err = syscall.Access(filePath, syscall.F_OK)
		if err != nil {
			log.Debug("file already deleted, filepath %s", filePath)
			self.DeletedMaxFileId = deleteFileId
			err = nil
			continue
		}

		err = os.Remove(filePath)
		if err != nil {
			log.Error("remove fail, file path %s error: %v", filePath, err)
			break
		}

		self.DeletedMaxFileId = deleteFileId
		self.FileLogger.Log("delete fileid %d", deleteFileId)
	}

	return err
}

func (self *LogStore) Del(fileIdStr string, instanceId uint64) error {
	var fileId int32 = -1
	var offset uint64
	var cksum uint32
	self.DecodeFileId(fileIdStr, &fileId, &offset, &cksum)

	if fileId > self.FileId {
		return fmt.Errorf("del fileid %d larger than using fileid %d", fileId, self.FileId)
	}

	if fileId > 0 {
		return self.DeleteFile(fileId - 1)
	}

	return nil
}

func (self *LogStore) ForceDel(fileIdStr string, instanceId uint64) error {
	var fileId int32
	var offset uint64
	var cksum uint32
	self.DecodeFileId(fileIdStr, &fileId, &offset, &cksum)

	if self.FileId != fileId {
		err := fmt.Errorf("del fileid %d not equal to fileid %d", fileId, self.FileId)
		log.Error("%v", err)
		return err
	}

	filePath := fmt.Sprintf("%s/%d.f", self.Path, fileId)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	file.Truncate(int64(offset))
	return nil
}

func (self *LogStore) IsValidFileId(fileIdStr string) bool {
	return len(fileIdStr) == (util.INT32SIZE + util.INT32SIZE + util.UINT32SIZE)
}

func (self *LogStore) GetFileId(needWriteSize uint32, fileId *int32, offset *uint32) error {
	var err error
	if self.File == nil {
		err = fmt.Errorf("file already broken, file id %d", self.FileId)
		log.Error("%v", err)
		return err
	}

	ret, err := self.File.Seek(int64(self.NowFileOffset), os.SEEK_SET)
	if err != nil {
		return err
	}
	*offset = uint32(ret)

	if uint64(*offset+needWriteSize) > self.NowFileSize {
		self.File.Close()
		self.File = nil

		err = self.IncreaseFileId()
		if err != nil {
			self.FileLogger.Log("new file increase fileid fail, now fileid %d", self.FileId)
			return err
		}

		self.File, err = self.OpenFile(self.FileId)
		if err != nil {
			self.FileLogger.Log("new file increase fileid fail, now fileid %d", self.FileId)
			return err
		}

		ret = -1
		ret, err = self.File.Seek(0, os.SEEK_END)
		if ret != 0 {
			self.FileLogger.Log("new file but file already exist,now file id %d exist filesize %d", self.FileId, ret)
			err = fmt.Errorf("Increase file id success, but file exist, data wrong, file size %d", ret)
			return err
		}
		*offset = uint32(ret)

		err = self.ExpendFile(self.File, &self.NowFileSize)
		if err != nil {
			err = fmt.Errorf("new file expand fail, file id %d", self.FileId)
			self.FileLogger.Log("new file expand file fail, now file id %d", self.FileId)
			self.File.Close()
			self.File = nil
			return err
		}

		self.FileLogger.Log("new file expand ok, file id %d filesize %d", self.FileId, self.NowFileSize)
	}

	*fileId = self.FileId
	return nil
}

func (self *LogStore) Append(options WriteOptions, instanceId uint64, buffer string, fileIdStr *string) error {
	begin := time.Now().UnixNano()

	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	bufferLen := len(buffer)
	len := util.UINT64SIZE + bufferLen
	tmpBufLen := len + util.INT32SIZE

	var fileId int32
	var offset uint32
	err := self.GetFileId(uint32(tmpBufLen), &fileId, &offset)
	if err != nil {
		return err
	}

	tmpBuf := make([]byte, tmpBufLen)
	util.EncodeInt32(tmpBuf, 0, int32(len))
	util.EncodeUint64(tmpBuf, util.INT32SIZE, instanceId)
	copy(tmpBuf[util.INT32SIZE+util.UINT64SIZE:], buffer)

	ret, err := self.File.Write(tmpBuf)
	if ret != tmpBufLen {
		err = fmt.Errorf("writelen %d not equal to %d,buffer size %d",
			ret, tmpBufLen, bufferLen)
		return err
	}

	if options.sync {
		self.File.Sync()
	}

	self.NowFileOffset += uint64(tmpBufLen)

	ckSum := util.Crc32(0, tmpBuf[util.INT32SIZE:], common.CRC32_SKIP)
	self.EncodeFileId(fileId, uint64(offset), ckSum, fileIdStr)

	useMs := (time.Now().UnixNano() - begin) / 1000000

	log.Info("ok, offset %d fileid %d cksum %d instanceid %d buffersize %d usetime %d ms sync %d",
		offset, fileId, ckSum, instanceId, useMs, bufferLen)
	return nil
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
