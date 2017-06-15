package logstorage

import (
  "fmt"
  "math"
  "math/rand"
  "os"
  "strconv"

  "github.com/syndtr/goleveldb/leveldb"
  "github.com/syndtr/goleveldb/leveldb/opt"

  "github.com/lichuang/gpaxos/common"
  "github.com/lichuang/gpaxos/log"
  "github.com/lichuang/gpaxos/util"
)

const MINCHOSEN_KEY = math.MaxUint64
const SYSTEMVARIABLES_KEY = math.MaxUint64 - 1
const MASTERVARIABLES_KEY = math.MaxUint64 - 2

type WriteOptions struct {
  Sync bool
}

type Database struct {
  valueStore *LogStore
  hasInit    bool
  myGroupIdx int32
  dbPath     string
  comparator PaxosComparator
  leveldb    *leveldb.DB
}

func (self *Database) ClearAllLog() error {
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

  err = self.Init(self.dbPath, self.myGroupIdx)
  if err != nil {
    log.Error("init again fail:%v", err)
    return err
  }

  options := WriteOptions{
    sync: true,
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

func (self *Database) Init(dbPath string, groupId int32) error {
  var err error
  if self.hasInit {
    return nil
  }

  self.myGroupIdx = groupId
  self.dbPath = dbPath

  options := opt.Options{
    ErrorIfMissing: false,
    Comparer:       &self.comparator,
    WriteBuffer:    int(1024*1024 + groupId*10*1024),
  }

  self.leveldb, err = leveldb.OpenFile(dbPath, &options)
  if err != nil {
    log.Error("open leveldb fail, db path:%s", dbPath)
    return err
  }

  self.valueStore = new(LogStore)
  err = self.valueStore.Init(dbPath, groupId, self)
  if err != nil {
    log.Error("value store init fail:%v", err)
    return err
  }

  self.hasInit = true
  log.Info("OK, db path:%s", dbPath)

  return nil
}

func (self *Database) GetDBPath() string {
  return self.dbPath
}

func (self *Database) GetMaxInstanceIDFileID(fileId *string, instanceId *uint64) error {
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

func (self *Database) RebuildOneIndex(instanceId uint64, fileIdstr string) error {
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

func (self *Database) GetFromLevelDb(instanceId uint64, value *[]byte) error {
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

  *value = ret
  return nil
}

func (self *Database) Get(instanceId uint64, value *string) error {
  var err error

  if !self.hasInit {
    err = fmt.Errorf("not init yet")
    return common.ErrDbNotInit
  }

  var fileId []byte
  err = self.GetFromLevelDb(instanceId, &fileId)
  if err != nil {
    return err
  }

  var fileInstanceId uint64
  err = self.FileIdToValue(string(fileId), &fileInstanceId, value)
  if err != nil {
    return err
  }

  if fileInstanceId != instanceId {
    log.Error("file instance id %d not equal to instance id %d", fileInstanceId, instanceId)
    return common.ErrInvalidInstanceId
  }

  return nil
}

func (self *Database) valueToFileId(options WriteOptions, instanceId uint64, value string, fileId *string) error {
  err := self.valueStore.Append(options, instanceId, value, fileId)
  if err != nil {
    log.Error("append fail:%v", err)
  }
  return err
}

func (self *Database) FileIdToValue(fileId string, instanceId *uint64, value *string) error {
  err := self.valueStore.Read(fileId, instanceId, value)
  if err != nil {
    log.Error("fail, ret %v", err)
    return err
  }

  return nil
}

func (self *Database) PutToLevelDB(sync bool, instanceId uint64, value []byte) error {
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

func (self *Database) Put(options WriteOptions, instanceId uint64, value string) error {
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

  return self.PutToLevelDB(false, instanceId, []byte(value))
}

func (self *Database) ForceDel(options WriteOptions, instanceId uint64) error {
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
    Sync: options.sync,
  }
  err = self.leveldb.Delete([]byte(key), &writeOptions)
  if err != nil {
    log.Error("leveldb.delete fail, instanceid %d, err:%v", instanceId, err)
    return err
  }
  return nil
}

func (self *Database) Del(options WriteOptions, instanceId uint64) error {
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
    Sync: options.sync,
  }
  err := self.leveldb.Delete([]byte(key), &writeOptions)
  if err != nil {
    log.Error("leveldb.delete fail, instanceid %d, err:%v", instanceId, err)
    return err
  }
  return nil
}

func (self *Database) GetMaxInstanceID(instanceId *uint64) error {
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

func (self *Database) GenKey(instanceId uint64) string {
  return fmt.Sprintf("%d", instanceId)
}

func (self *Database) GetInstanceIDFromKey(key string) uint64 {
  instanceId, _ := strconv.ParseUint(key, 10, 64)
  return instanceId
}

func (self *Database) GetMinChosenInstanceID(minInstanceId *uint64) error {
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
  var sValue string
  if self.valueStore.IsValidFileId(string(value)) {
    err = self.Get(minKey, &sValue)
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

func (self *Database) SetMinChosenInstanceID(options WriteOptions, minInstanceId uint64) error {
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

func (self *Database) SetSystemVariables(options WriteOptions, buffer []byte) error {
  return self.PutToLevelDB(true, SYSTEMVARIABLES_KEY, buffer)
}

func (self *Database) GetSystemVariables(buffer *[]byte) error {
  return self.GetFromLevelDb(SYSTEMVARIABLES_KEY, buffer)
}

func (self *Database) SetMasterVariables(options WriteOptions, buffer []byte) error {
  return self.PutToLevelDB(true, MASTERVARIABLES_KEY, buffer)
}

func (self *Database) GetMasterVariables(buffer *[]byte) error {
  return self.GetFromLevelDb(MASTERVARIABLES_KEY, buffer)
}
