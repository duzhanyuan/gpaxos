package logstorage

import (
  "github.com/syndtr/goleveldb/leveldb"
  "os"
  "github.com/syndtr/goleveldb/leveldb/opt"
  "fmt"
  "strconv"

  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/util"
  "math"
  "math/rand"
  log "github.com/lichuang/log4go"
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

const MINCHOSEN_KEY = math.MaxUint64 - 1
const SYSTEMVARIABLES_KEY = MINCHOSEN_KEY - 1
const MASTERVARIABLES_KEY = MINCHOSEN_KEY - 2

type WriteOptions struct {
  Sync bool
}

type LogStorage struct {
  valueStore *ValueStore
  hasInit    bool
  dbPath     string
  comparator PaxosComparator
  leveldb    *leveldb.DB
}

func (self *LogStorage) ClearAllLog() error {
  sysVarbuffer, err := self.GetSystemVariables()
  if err != nil && err != common.ErrKeyNotFound {
    log.Error("GetSystemVariables fail, ret %v", err)
    return err
  }

  masVarbuffer, err := self.GetMasterVariables()
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

  self.valueStore = new(ValueStore)
  err = self.valueStore.Init(dbPath, self)
  if err != nil {
    log.Error("value store init fail:%v", err)
    return err
  }

  self.hasInit = true
  log.Info("OK, db path:%s", dbPath)

  return nil
}

func (self *LogStorage) Close() {
  self.leveldb.Close()
  self.valueStore.Close()
}

func (self *LogStorage) GetDBPath() string {
  return self.dbPath
}

func (self *LogStorage) GetMaxInstanceIDFileID()(string, uint64, error) {
  maxInstanceId, err := self.GetMaxInstanceID()
  if err != nil {
    return "", 0, nil
  }

  key := self.genKey(maxInstanceId)
  value, err := self.leveldb.Get([]byte(key), &opt.ReadOptions{})
  if err != nil {
    if err == leveldb.ErrNotFound {
      return "", 0, common.ErrKeyNotFound
    }

    log.Error("leveldb.get fail:%v", err)
    return "", 0, common.ErrGetFail
  }

  return string(value), maxInstanceId, nil
}

func (self *LogStorage) rebuildOneIndex(instanceId uint64, fileIdstr string) error {
  key := self.genKey(instanceId)

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

func (self *LogStorage) getFromLevelDb(instanceId uint64) ([]byte, error) {
  key := self.genKey(instanceId)

  ret, err := self.leveldb.Get([]byte(key), nil)
  if err != nil {
    if err == leveldb.ErrNotFound {
      log.Debug("leveldb.get not found, instanceid %d", instanceId)
      return nil, common.ErrKeyNotFound
    }

    log.Error("leveldb.get fail, instanceid %d", instanceId)
    return nil, common.ErrGetFail
  }

  return ret, nil
}

func (self *LogStorage) Get(instanceId uint64) ([]byte, error) {
  var err error

  if !self.hasInit {
    err = fmt.Errorf("not init yet")
    return nil, common.ErrDbNotInit
  }

  fileId, err := self.getFromLevelDb(instanceId)
  if err != nil {
    return nil, err
  }

  var fileInstanceId uint64
  value, err := self.fileIdToValue(string(fileId), &fileInstanceId)
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

func (self *LogStorage) fileIdToValue(fileId string, instanceId *uint64) ([]byte, error) {
  value, err := self.valueStore.Read(fileId, instanceId)
  if err != nil {
    log.Error("fail, ret %v", err)
    return nil, err
  }

  return value, nil
}

func (self *LogStorage) putToLevelDB(sync bool, instanceId uint64, value []byte) error {
  key := self.genKey(instanceId)

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

  return self.putToLevelDB(false, instanceId, []byte(fileId))
}

func (self *LogStorage) ForceDel(options WriteOptions, instanceId uint64) error {
  if !self.hasInit {
    log.Error("no init yet")
    return common.ErrDbNotInit
  }

  key := self.genKey(instanceId)
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

  key := self.genKey(instanceId)

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

func (self *LogStorage) GetMaxInstanceID()(uint64, error) {
  var instanceId uint64 = MINCHOSEN_KEY
  iter := self.leveldb.NewIterator(nil, &opt.ReadOptions{})

  iter.Last()

  for {
    if !iter.Valid() {
      break
    }

    instanceId = self.getInstanceIDFromKey(string(iter.Key()))
    if instanceId == MINCHOSEN_KEY || instanceId == SYSTEMVARIABLES_KEY || instanceId == MASTERVARIABLES_KEY {
      iter.Prev()
    } else {
      return instanceId, nil
    }
  }

  return common.INVALID_INSTANCEID, common.ErrKeyNotFound
}

func (self *LogStorage) genKey(instanceId uint64) string {
  return fmt.Sprintf("%d", instanceId)
}

func (self *LogStorage) getInstanceIDFromKey(key string) uint64 {
  instanceId, _ := strconv.ParseUint(key, 10, 64)
  return instanceId
}

func (self *LogStorage) GetMinChosenInstanceID() (uint64, error) {
  if !self.hasInit {
    log.Error("db not init yet")
    return common.INVALID_INSTANCEID, common.ErrDbNotInit
  }

  value, err := self.getFromLevelDb(MINCHOSEN_KEY)
  if err != nil && err != common.ErrKeyNotFound {
    return common.INVALID_INSTANCEID, err
  }

  if err == common.ErrKeyNotFound {
    log.Error("no min chosen instanceid")
    return 0, nil
  }

  if len(value) != util.UINT64SIZE {
    log.Error("fail, mininstanceid size wrong")
    return common.INVALID_INSTANCEID, common.ErrInvalidInstanceId
  }

  var minInstanceId uint64
  util.DecodeUint64([]byte(value), 0, &minInstanceId)
  log.Info("ok, min chosen instanceid:%d", minInstanceId)
  return minInstanceId, nil
}

func (self *LogStorage) SetMinChosenInstanceID(minInstanceId uint64) error {
  if !self.hasInit {
    log.Error("no init yet")
    return common.ErrDbNotInit
  }

  var value []byte = make([]byte, util.UINT64SIZE)
  util.EncodeUint64(value, 0, minInstanceId)

  err := self.putToLevelDB(true, MINCHOSEN_KEY, value)
  if err != nil {
    return err
  }

  log.Info("ok, min chosen instanceid %d", minInstanceId)
  return nil
}

func (self *LogStorage) SetSystemVariables(options WriteOptions, buffer []byte) error {
  return self.putToLevelDB(true, SYSTEMVARIABLES_KEY, buffer)
}

func (self *LogStorage) GetSystemVariables()([]byte, error) {
  return self.getFromLevelDb(SYSTEMVARIABLES_KEY)
}

func (self *LogStorage) SetMasterVariables(options WriteOptions, buffer []byte) error {
  return self.putToLevelDB(true, MASTERVARIABLES_KEY, buffer)
}

func (self *LogStorage) GetMasterVariables()([]byte,error) {
  return self.getFromLevelDb(MASTERVARIABLES_KEY)
}