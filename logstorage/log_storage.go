package logstorage

import (
  "github.com/syndtr/goleveldb/leveldb"
  "os"
  "github.com/syndtr/goleveldb/leveldb/opt"
  "fmt"
  "strconv"

  "github.com/lichuang/gpaxos/common"
  log "github.com/lichuang/log4go"
  "github.com/lichuang/gpaxos/util"
  "math"
  "math/rand"
)

/*
  In leveldb, there are instance data index, the instance data itself
  saved in file.

  leveldb data:
    key - instance
    value format - fileid(int32)+file offset(uint64)+cksum of file value(uint32)

  meta file format(data path/vpath/meta):
    current file id(int32)
    file id cksum(uint32)

  data file(data path/vpath/fileid.f) data format:
    data len(int32)
    value(data len) format:
      instance id(uint64)
      acceptor state data(data len - sizeof(uint64))
 */

const MINCHOSEN_KEY = math.MaxUint64
const SYSTEMVARIABLES_KEY = math.MaxUint64 - 1
const MASTERVARIABLES_KEY = math.MaxUint64 - 2

type WriteOptions struct {
  Sync bool
}

type LogStorage struct {
  valueStore *LogStore
  hasInit    bool
  dbPath     string
  comparator PaxosComparator
  leveldb    *leveldb.DB
}

func (self *LogStorage) ClearAllLog() error {
  var sysVarbuffer []byte
  err := self.GetSystemVariables(&sysVarbuffer)
  if err != nil && err != common.ErrKeyNotFound {
    log.Error("GetSystemVariables fail, ret %v", err)
    return err
  }

  var masVarbuffer []byte
  err = self.GetMasterVariables(&masVarbuffer)
  if err != nil && err != common.ErrKeyNotFound {
    log.Error("GetMasterVariables fail, ret %v", err)
    return err
  }

  self.hasInit = false
  self.leveldb = nil
  self.valueStore = nil

  bakPath := self.dbPath + ".bak"
  err = util.DeleteDir(bakPath)
  if err != nil {
    log.Error("delete bak dir %s fail:%v", bakPath, err)
    return err
  }

  os.Rename(self.dbPath, bakPath)

  err = self.Init(self.dbPath)
  if err != nil {
    log.Error("init again fail:%v", err)
    return err
  }

  options := WriteOptions{
    Sync: true,
  }
  if len(sysVarbuffer) > 0 {
    err = self.SetSystemVariables(options, sysVarbuffer)
    if err != nil {
      log.Error("SetSystemVariables fail:%v", err)
      return err
    }
  }
  if len(masVarbuffer) > 0 {
    err = self.SetMasterVariables(options, masVarbuffer)
    if err != nil {
      log.Error("SetMasterVariables fail:%v", err)
      return err
    }
  }

  return nil
}

func (self *LogStorage) Init(dbPath string) error {
  var err error
  if self.hasInit {
    return nil
  }

  self.dbPath = dbPath

  options := opt.Options{
    ErrorIfMissing: false,
    Comparer:       &self.comparator,
    WriteBuffer:    int(1024*1024),
  }

  self.leveldb, err = leveldb.OpenFile(dbPath, &options)
  if err != nil {
    log.Error("open leveldb fail, db path:%s", dbPath)
    return err
  }

  self.valueStore = new(LogStore)
  err = self.valueStore.Init(dbPath, self)
  if err != nil {
    log.Error("value store init fail:%v", err)
    return err
  }

  self.hasInit = true
  log.Info("OK, db path:%s", dbPath)

  return nil
}

func (self *LogStorage) GetDBPath() string {
  return self.dbPath
}

func (self *LogStorage) GetMaxInstanceIDFileID(fileId *string, instanceId *uint64) error {
  var maxInstanceId uint64

  err := self.GetMaxInstanceID(&maxInstanceId)
  if err != nil {
    *fileId = ""
    return nil
  }

  key := self.GenKey(maxInstanceId)
  value, err := self.leveldb.Get([]byte(key), &opt.ReadOptions{})
  if err != nil {
    if err == leveldb.ErrNotFound {
      return common.ErrKeyNotFound
    }

    log.Error("leveldb.get fail:%v", err)
    return common.ErrGetFail
  }

  *fileId = string(value)
  *instanceId = maxInstanceId

  return nil
}

func (self *LogStorage) RebuildOneIndex(instanceId uint64, fileIdstr string) error {
  key := self.GenKey(instanceId)

  opt := opt.WriteOptions{
    Sync: false,
  }

  err := self.leveldb.Put([]byte(key), []byte(fileIdstr), &opt)
  if err != nil {
    log.Error("leveldb.Put fail, instanceid %d valuelen %d", instanceId, len(fileIdstr))
    return err
  }
  return nil
}

func (self *LogStorage) GetFromLevelDb(instanceId uint64, value *[]byte) error {
  key := self.GenKey(instanceId)

  ret, err := self.leveldb.Get([]byte(key), nil)
  if err != nil {
    if err == leveldb.ErrNotFound {
      log.Error("leveldb.get not found, instanceid %d", instanceId)
      return common.ErrKeyNotFound
    }

    log.Error("leveldb.get fail, instanceid %d", instanceId)
    return common.ErrGetFail
  }

  log.Info("ret:%v", string(ret))
  *value = ret
  return nil
}

func (self *LogStorage) Get(instanceId uint64) ([]byte, error) {
  var err error

  if !self.hasInit {
    err = fmt.Errorf("not init yet")
    return nil, common.ErrDbNotInit
  }

  var fileId []byte
  err = self.GetFromLevelDb(instanceId, &fileId)
  if err != nil {
    return nil, err
  }
  log.Info("ret:%v", string(fileId))

  var fileInstanceId uint64
  value, err := self.FileIdToValue(string(fileId), &fileInstanceId)
  if err != nil {
    return nil, err
  }

  if fileInstanceId != instanceId {
    log.Error("file instance id %d not equal to instance id %d", fileInstanceId, instanceId)
    return nil, common.ErrInvalidInstanceId
  }

  return value, nil
}

func (self *LogStorage) valueToFileId(options WriteOptions, instanceId uint64, value []byte, fileId *string) error {
  err := self.valueStore.Append(options, instanceId, value, fileId)
  if err != nil {
    log.Error("append fail:%v", err)
  }
  return err
}

func (self *LogStorage) FileIdToValue(fileId string, instanceId *uint64) ([]byte, error) {
  value, err := self.valueStore.Read(fileId, instanceId)
  if err != nil {
    log.Error("fail, ret %v", err)
    return nil, err
  }

  return value, nil
}

func (self *LogStorage) PutToLevelDB(sync bool, instanceId uint64, value []byte) error {
  key := self.GenKey(instanceId)

  options := opt.WriteOptions{
    Sync: sync,
  }

  err := self.leveldb.Put([]byte(key), value, &options)
  if err != nil {
    log.Error("leveldb put fail, instanceid %d value len %d", instanceId, len(value))
    return err
  }

  return nil
}

func (self *LogStorage) Put(options WriteOptions, instanceId uint64, value []byte) error {
  var err error

  if !self.hasInit {
    err = fmt.Errorf("not init yet")
    return err
  }

  var fileId string
  err = self.valueToFileId(options, instanceId, value, &fileId)
  if err != nil {
    return err
  }

  return self.PutToLevelDB(false, instanceId, []byte(fileId))
}

func (self *LogStorage) ForceDel(options WriteOptions, instanceId uint64) error {
  if !self.hasInit {
    log.Error("no init yet")
    return common.ErrDbNotInit
  }

  key := self.GenKey(instanceId)
  fileId, err := self.leveldb.Get([]byte(key), &opt.ReadOptions{})
  if err != nil {
    if err == leveldb.ErrNotFound {
      log.Error("leveldb.get not found, instance:%d", instanceId)
      return nil
    }
    log.Error("leveldb.get fail:%v", err)
    return common.ErrGetFail
  }

  err = self.valueStore.ForceDel(string(fileId), instanceId)
  if err != nil {
    return err
  }

  writeOptions := opt.WriteOptions{
    Sync: options.Sync,
  }
  err = self.leveldb.Delete([]byte(key), &writeOptions)
  if err != nil {
    log.Error("leveldb.delete fail, instanceid %d, err:%v", instanceId, err)
    return err
  }
  return nil
}

func (self *LogStorage) Del(options WriteOptions, instanceId uint64) error {
  if !self.hasInit {
    log.Error("no init yet")
    return common.ErrDbNotInit
  }

  key := self.GenKey(instanceId)

  if rand.Intn(100) < 10 {
    fileId, err := self.leveldb.Get([]byte(key), &opt.ReadOptions{})
    if err != nil {
      if err == leveldb.ErrNotFound {
        log.Error("leveldb.get not found, instance:%d", instanceId)
        return nil
      }
      log.Error("leveldb.get fail:%v", err)
      return common.ErrGetFail
    }

    err = self.valueStore.Del(string(fileId), instanceId)
    if err != nil {
      return err
    }

  }
  writeOptions := opt.WriteOptions{
    Sync: options.Sync,
  }
  err := self.leveldb.Delete([]byte(key), &writeOptions)
  if err != nil {
    log.Error("leveldb.delete fail, instanceid %d, err:%v", instanceId, err)
    return err
  }
  return nil
}

func (self *LogStorage) GetMaxInstanceID(instanceId *uint64) error {
  *instanceId = MINCHOSEN_KEY
  iter := self.leveldb.NewIterator(nil, &opt.ReadOptions{})

  iter.Last()

  for {
    if !iter.Valid() {
      break
    }

    *instanceId = self.GetInstanceIDFromKey(string(iter.Key()))
    if *instanceId == MINCHOSEN_KEY || *instanceId == SYSTEMVARIABLES_KEY || *instanceId == MASTERVARIABLES_KEY {
      iter.Prev()
    } else {
      return nil
    }
  }

  return common.ErrKeyNotFound
}

func (self *LogStorage) GenKey(instanceId uint64) string {
  return fmt.Sprintf("%d", instanceId)
}

func (self *LogStorage) GetInstanceIDFromKey(key string) uint64 {
  instanceId, _ := strconv.ParseUint(key, 10, 64)
  return instanceId
}

func (self *LogStorage) GetMinChosenInstanceID(minInstanceId *uint64) error {
  if !self.hasInit {
    log.Error("db not init yet")
    return common.ErrDbNotInit
  }

  var value []byte
  err := self.GetFromLevelDb(MINCHOSEN_KEY, &value)
  if err != common.ErrGetFail {
    return err
  }

  if err == common.ErrKeyNotFound {
    log.Error("no min chsoen instanceid")
    *minInstanceId = 0
    return nil
  }

  var minKey uint64 = MINCHOSEN_KEY
  var sValue []byte
  if self.valueStore.IsValidFileId(string(value)) {
    sValue, err = self.Get(minKey)
    if err != nil {
      log.Error("get from long store fail:%v", err)
      return err
    }
  }

  if len(sValue) != util.UINT64SIZE {
    log.Error("fail, mininstanceid size wrong")
    return common.ErrInvalidInstanceId
  }

  util.DecodeUint64([]byte(sValue), 0, minInstanceId)
  log.Info("ok, min chosen instanceid:%d", *minInstanceId)
  return nil
}

func (self *LogStorage) SetMinChosenInstanceID(options WriteOptions, minInstanceId uint64) error {
  if !self.hasInit {
    log.Error("no init yet")
    return common.ErrDbNotInit
  }

  var minKey uint64 = MINCHOSEN_KEY
  var value []byte
  util.EncodeUint64(value, 0, minInstanceId)

  err := self.PutToLevelDB(true, minKey, value)
  if err != nil {
    return err
  }

  log.Info("ok, min chosen instanceid %d", minInstanceId)
  return nil
}

func (self *LogStorage) SetSystemVariables(options WriteOptions, buffer []byte) error {
  return self.PutToLevelDB(true, SYSTEMVARIABLES_KEY, buffer)
}

func (self *LogStorage) GetSystemVariables(buffer *[]byte) error {
  return self.GetFromLevelDb(SYSTEMVARIABLES_KEY, buffer)
}

func (self *LogStorage) SetMasterVariables(options WriteOptions, buffer []byte) error {
  return self.PutToLevelDB(true, MASTERVARIABLES_KEY, buffer)
}

func (self *LogStorage) GetMasterVariables(buffer *[]byte) error {
  return self.GetFromLevelDb(MASTERVARIABLES_KEY, buffer)
}