package logstorage

import (
  "bytes"
  "fmt"
  "os"
  "strconv"
  "sync"
  "syscall"
  "time"

  "github.com/golang/protobuf/proto"
  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/util"

  log "github.com/lichuang/log4go"
)

type fileLogger struct {
  file *os.File
}

func (self *fileLogger) Init(path string) {
  filePath := fmt.Sprintf("%s/LOG", path)
  self.file, _ = os.Open(filePath)
}

func (self *fileLogger) Log(format string, args ...interface{}) {
  if self.file == nil {
    return
  }

  nowTimeMs := util.NowTimeMs()
  timePrefix := time.Now().Format("2006-01-02 15:04:05")

  prefix := fmt.Sprintf("%s:%d", timePrefix, nowTimeMs%1000)
  newFormat := prefix + format + "\n"

  buf := fmt.Sprintf(newFormat, args)
  self.file.Write([]byte(buf))
}

type ValueStore struct {
  path             string
  fileLogger       fileLogger
  metaFile         *os.File
  fileId           int32
  nowFileOffset    uint64
  nowFileSize      uint64
  file             *os.File
  readMutex        sync.Mutex
  mutex            sync.Mutex
  deletedMaxFileId int32
}

func (self *ValueStore) Init(path string, db *LogStorage) error {
  self.deletedMaxFileId = -1
  self.path = path + "/vfile"
  self.file = nil
  self.fileId = -1

  err := syscall.Access(self.path, syscall.F_OK)
  if err != nil {
    err := os.Mkdir(self.path, os.ModePerm)
    if err != nil {
      return fmt.Errorf("create dir %s error: %v", self.path, err)
    }
  }

  self.fileLogger.Init(self.path)

  // decode file id from meta file
  // meta file format:
  //  current file id(int32)
  //  file id cksum(uint32)
  var metaFilePath = self.path + "/meta"
  metaFile, err := os.OpenFile(metaFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
  if err != nil {
    return fmt.Errorf("open meta file %s error: %v", metaFilePath, err)
  }
  self.metaFile = metaFile

  _, err = metaFile.Seek(0, os.SEEK_SET)
  if err != nil {
    return fmt.Errorf("seek file %s error %v", metaFile.Name(), err)
  }

  buff := make([]byte, util.INT32SIZE)
  n, err := metaFile.Read(buff)
  if n != util.INT32SIZE {
    if n == 0 { // no content
      self.fileId = 0
    } else {
      return fmt.Errorf("read meta file %s info fail, real len %d", metaFile.Name(), n)
    }
  }
  util.DecodeInt32(buff, 0, &self.fileId)
  ckSum := util.Crc32(0, buff, 0)

  var metaCkSum uint32
  buff = make([]byte, util.UINT32SIZE)
  n, err = metaFile.Read(buff)
  if n == util.UINT32SIZE {
    util.DecodeUint32(buff, 0, &metaCkSum)
    if metaCkSum != ckSum {
      return fmt.Errorf("meta file checksum %d not same to calc checksum %d, file id %d",
        metaCkSum, ckSum, self.fileId)
    }
  }

  err = self.rebuildIndex(db, &self.nowFileOffset)
  if err != nil {
    return err
  }

  self.file, err = self.OpenFile(self.fileId)
  if err != nil {
    return err
  }

  err = self.expendFile(self.file, &self.nowFileSize)
  if err != nil {
    return err
  }

  nowFileOffset, err := self.file.Seek(int64(self.nowFileOffset), os.SEEK_SET)
  if err != nil {
    return err
  }
  self.nowFileOffset = uint64(nowFileOffset)

  self.fileLogger.Log("init write fileid %d now_write_offset %d filesize %d",
    self.fileId, self.nowFileOffset, self.nowFileSize)

  log.Info("ok, path %s fileid %d meta cksum %d nowfilesize %d nowfilewriteoffset %d",
    self.path, self.fileId, metaCkSum, self.nowFileSize, self.nowFileOffset)

  return nil
}

func (self *ValueStore) Close() {
  self.metaFile.Close()
  self.file.Close()
}

func (self *ValueStore) expendFile(file *os.File, fileSize *uint64) error {
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
    self.nowFileOffset = 0
  }

  return nil
}

func (self *ValueStore) IncreaseFileId() error {
  fileId := self.fileId + 1
  buffer := make([]byte, util.INT32SIZE)
  util.EncodeInt32(buffer, 0, fileId)

  _, err := self.metaFile.Seek(0, os.SEEK_SET)
  if err != nil {
    return err
  }

  n, err := self.metaFile.Write(buffer)
  if err != nil {
    return err
  }
  if n != util.INT32SIZE {
    return fmt.Errorf("write len %d not equal to %d", n, util.INT32SIZE)
  }
  ckSum := util.Crc32(0, buffer, 0)
  buffer = make([]byte, util.UINT32SIZE)
  util.EncodeUint32(buffer, 0, ckSum)
  n, err = self.metaFile.Write(buffer)
  if err != nil {
    return err
  }

  if n != util.UINT32SIZE {
    return fmt.Errorf("write len %d not equal to %d", n, util.UINT32SIZE)
  }

  self.metaFile.Sync()

  self.fileId += 1
  return nil
}

func (self *ValueStore) OpenFile(fileId int32) (*os.File, error) {
  filePath := fmt.Sprintf("%s/%d.f", self.path, fileId)
  return os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func (self *ValueStore) DeleteFile(fileId int32) error {
  if self.deletedMaxFileId == -1 {
    if fileId-2000 > 0 {
      self.deletedMaxFileId = fileId - 2000
    }
  }

  if fileId <= self.deletedMaxFileId {
    log.Debug("file already deleted, fileid %d deletedmaxfileid %d", fileId, self.deletedMaxFileId)
    return nil
  }

  var err error
  for deleteFileId := self.deletedMaxFileId + 1; deleteFileId <= fileId; deleteFileId++ {
    filePath := fmt.Sprintf("%s/%d.f", self.path, deleteFileId)

    err = syscall.Access(filePath, syscall.F_OK)
    if err != nil {
      log.Debug("file already deleted, filepath %s", filePath)
      self.deletedMaxFileId = deleteFileId
      err = nil
      continue
    }

    err = os.Remove(filePath)
    if err != nil {
      log.Error("remove fail, file path %s error: %v", filePath, err)
      break
    }

    self.deletedMaxFileId = deleteFileId
    self.fileLogger.Log("delete fileid %d", deleteFileId)
  }

  return err
}

func (self *ValueStore) getFileId(needWriteSize uint32, fileId *int32, offset *uint32) error {
  var err error
  if self.file == nil {
    err = fmt.Errorf("file already broken, file id %d", self.fileId)
    log.Error("%v", err)
    return err
  }

  ret, err := self.file.Seek(int64(self.nowFileOffset), os.SEEK_SET)
  if err != nil {
    return err
  }
  *offset = uint32(ret)

  if uint64(*offset+needWriteSize) > self.nowFileSize {
    self.file.Close()
    self.file = nil

    err = self.IncreaseFileId()
    if err != nil {
      self.fileLogger.Log("new file increase fileid fail, now fileid %d", self.fileId)
      return err
    }

    self.file, err = self.OpenFile(self.fileId)
    if err != nil {
      self.fileLogger.Log("new file increase fileid fail, now fileid %d", self.fileId)
      return err
    }

    ret = -1
    ret, err = self.file.Seek(0, os.SEEK_END)
    if ret != 0 {
      self.fileLogger.Log("new file but file already exist,now file id %d exist filesize %d", self.fileId, ret)
      err = fmt.Errorf("Increase file id success, but file exist, data wrong, file size %d", ret)
      return err
    }
    *offset = uint32(ret)

    err = self.expendFile(self.file, &self.nowFileSize)
    if err != nil {
      err = fmt.Errorf("new file expand fail, file id %d", self.fileId)
      self.fileLogger.Log("new file expand file fail, now file id %d", self.fileId)
      self.file.Close()
      self.file = nil
      return err
    }

    self.fileLogger.Log("new file expand ok, file id %d filesize %d", self.fileId, self.nowFileSize)
  }

  *fileId = self.fileId
  return nil
}

// data file(data path/vpath/fileid.f) data format:
//  data len(int32)
//  value(data len) format:
//    instance id(uint64)
//    acceptor state data(data len - sizeof(uint64))
func (self *ValueStore) Append(options WriteOptions, instanceId uint64, buffer []byte, fileIdStr *string) error {
  begin := util.NowTimeMs()

  self.mutex.Lock()
  defer self.mutex.Unlock()

  bufferLen := len(buffer)
  len := util.UINT64SIZE + bufferLen
  tmpBufLen := len + util.INT32SIZE

  var fileId int32
  var offset uint32
  err := self.getFileId(uint32(tmpBufLen), &fileId, &offset)
  if err != nil {
    return err
  }

  tmpBuf := make([]byte, tmpBufLen)
  util.EncodeInt32(tmpBuf, 0, int32(len))
  util.EncodeUint64(tmpBuf, util.INT32SIZE, instanceId)
  copy(tmpBuf[util.INT32SIZE+util.UINT64SIZE:], []byte(buffer))

  ret, err := self.file.Write(tmpBuf)
  if ret != tmpBufLen {
    err = fmt.Errorf("writelen %d not equal to %d,buffer size %d",
      ret, tmpBufLen, bufferLen)
    return err
  }

  if options.Sync {
    self.file.Sync()
  }

  self.nowFileOffset += uint64(tmpBufLen)

  ckSum := util.Crc32(0, tmpBuf[util.INT32SIZE:], common.CRC32_SKIP)
  self.EncodeFileId(fileId, uint64(offset), ckSum, fileIdStr)

  useMs := util.NowTimeMs() - begin

  log.Info("ok, offset %d fileid %d cksum %d instanceid %d buffersize %d usetime %d ms sync %t",
    offset, fileId, ckSum, instanceId, bufferLen, useMs, options.Sync)
  return nil
}

func (self *ValueStore) Read(fileIdstr string, instanceId *uint64) ([]byte, error) {
  var fileId int32
  var offset uint64
  var cksum uint32
  self.DecodeFileId(fileIdstr, &fileId, &offset, &cksum)

  file, err := self.OpenFile(fileId)
  if err != nil {
    log.Error("openfile %s error %v", fileId, err)
    return nil, err
  }

  _, err = file.Seek(int64(offset), os.SEEK_SET)
  if err != nil {
    return nil, err
  }

  tmpbuf := make([]byte, util.INT32SIZE)
  n, err := file.Read(tmpbuf)
  if err != nil {
    return nil, err
  }
  if n != util.INT32SIZE {
    return nil, fmt.Errorf("read len %d not equal to %d", n, util.INT32SIZE)
  }

  var bufferlen int32
  util.DecodeInt32(tmpbuf, 0, &bufferlen)

  self.readMutex.Lock()
  defer self.readMutex.Unlock()

  tmpbuf = make([]byte, bufferlen)
  n, err = file.Read(tmpbuf)
  if err != nil {
    return nil, err
  }

  if n != int(bufferlen) {
    return nil, fmt.Errorf("read len %d not equal to %d", n, bufferlen)
  }

  fileCkSum := util.Crc32(0, tmpbuf, common.CRC32_SKIP)
  if fileCkSum != cksum {
    return nil, fmt.Errorf("cksum not equal, file cksum %d, cksum %d", fileCkSum, cksum)
  }

  util.DecodeUint64(tmpbuf, 0, instanceId)


  log.Info("ok, fileid %d offset %d instanceid %d buffser size %d",
    fileId, offset, *instanceId, int(bufferlen)-util.UINT64SIZE)

  return tmpbuf[util.UINT64SIZE:], nil
}

func (self *ValueStore) Del(fileIdStr string, instanceId uint64) error {
  var fileId int32 = -1
  var offset uint64
  var cksum uint32
  self.DecodeFileId(fileIdStr, &fileId, &offset, &cksum)

  if fileId > self.fileId {
    return fmt.Errorf("del fileid %d larger than using fileid %d", fileId, self.fileId)
  }

  if fileId > 0 {
    return self.DeleteFile(fileId - 1)
  }

  return nil
}

func (self *ValueStore) ForceDel(fileIdStr string, instanceId uint64) error {
  var fileId int32
  var offset uint64
  var cksum uint32
  self.DecodeFileId(fileIdStr, &fileId, &offset, &cksum)

  if self.fileId != fileId {
    err := fmt.Errorf("del fileid %d not equal to fileid %d", fileId, self.fileId)
    log.Error("%v", err)
    return err
  }

  filePath := fmt.Sprintf("%s/%d.f", self.path, fileId)

  file, err := os.Open(filePath)
  if err != nil {
    return err
  }

  file.Truncate(int64(offset))
  return nil
}

func (self *ValueStore) EncodeFileId(fileId int32, offset uint64, cksum uint32, fileIdStr *string) {
  buffer := make([]byte, util.INT32SIZE+util.UINT64SIZE+util.UINT32SIZE)
  util.EncodeInt32(buffer, 0, fileId)
  util.EncodeUint64(buffer, util.INT32SIZE, offset)
  util.EncodeUint32(buffer, util.INT32SIZE+util.UINT64SIZE, cksum)

  *fileIdStr = string(buffer)
}

func (self *ValueStore) DecodeFileId(fileIdStr string, fileId *int32, offset *uint64, cksum *uint32) {
  buffer := bytes.NewBufferString(fileIdStr).Bytes()

  util.DecodeInt32(buffer, 0, fileId)
  util.DecodeUint64(buffer, util.INT32SIZE, offset)
  util.DecodeUint32(buffer, util.INT32SIZE+util.UINT64SIZE, cksum)
}

func (self *ValueStore) isValidFileId(fileIdStr string) bool {
  return len(fileIdStr) == (util.INT32SIZE + util.UINT64SIZE + util.UINT32SIZE)
}

func (self *ValueStore) rebuildIndex(db *LogStorage, nowOffset *uint64) error {
  // 1. get max instance id and file id from leveldb
  lastFileId, nowInstanceId, err := db.GetMaxInstanceIDFileID()
  if err != nil {
    return err
  }

  // 2. decode last file id info
  var fileId int32
  var offset uint64
  var cksum uint32
  if len(lastFileId) > 0 {
    self.DecodeFileId(lastFileId, &fileId, &offset, &cksum)
  }

  if fileId > self.fileId {
    return fmt.Errorf("leveldb last fileid %d lagger than meta now fileid %d", fileId, self.fileId)
  }

  log.Info("start fileid %d offset %d checksum %d", fileId, offset, cksum)

  for nowFileId := fileId; ; nowFileId++ {
    err = self.RebuildIndexForOneFile(nowFileId, offset, db, nowOffset, &nowInstanceId)
    if err != nil {
      if err == common.ErrFileNotExist {
        if nowFileId != 0 && nowFileId != self.fileId+1 {
          err = fmt.Errorf("meta file wrong, now file id %d meta file id %d", nowFileId, self.fileId)
          log.Error("%v", err)
          return common.ErrInvalidMetaFileId
        }
        log.Info("end rebuild ok, now file id:%d", nowFileId)
        err = nil
      }
      break
    }

    offset = 0
  }

  return err
}

func (self *ValueStore) RebuildIndexForOneFile(fileId int32, offset uint64, db *LogStorage, nowWriteOffset *uint64, nowInstanceId *uint64) error {
  var err error = nil

  var file *os.File = nil
  defer func() {
    if file != nil {
      file.Close()
    }
  }()
  filePath := fmt.Sprintf("%s/%d.f", self.path, fileId)

  err = syscall.Access(filePath, syscall.F_OK)
  if err != nil {
    log.Debug("file %s not exist", filePath)
    return common.ErrFileNotExist
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
      err = common.ErrInvalidInstanceId
      break
    }

    *nowInstanceId = instanceId

    var state common.AcceptorStateData
    err = proto.Unmarshal(buffer[util.UINT64SIZE:], &state)
    if err != nil {
      self.nowFileOffset = uint64(nowOffset)
      needTruncate = true
      log.Error("this instance buffer wrong, can't parse to acceptState, instanceid %d bufferlen %d nowoffset %d",
        instanceId, len-util.UINT64SIZE, nowOffset)
      err = nil
      break
    }

    fileCkSum := util.Crc32(0, buffer, common.CRC32_SKIP)
    var fileIdstr string
    self.EncodeFileId(fileId, nowOffset, fileCkSum, &fileIdstr)

    err = db.rebuildOneIndex(instanceId, fileIdstr)
    if err != nil {
      break
    }

    log.Info("rebuild one index ok, fileid %d offset %d instanceid %d cksum %d buffer size %d",
      fileId, nowOffset, instanceId, fileCkSum, len-util.UINT64SIZE)

    nowOffset += uint64(util.INT32SIZE) + uint64(len)
  }

  if needTruncate {
    self.fileLogger.Log("truncate fileid %d offset %d filesize %d", fileId, nowOffset, fileLen)
    err = os.Truncate(filePath, int64(nowOffset))
    if err != nil {
      log.Error("truncate fail, file path %s truncate to length %d error:%v",
        filePath, nowOffset, err)
      return err
    }
  }
  return err
}
