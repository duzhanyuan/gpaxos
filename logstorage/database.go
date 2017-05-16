package logstorage

import (
	"bytes"
	"fmt"

	leveldb "github.com/syndtr/goleveldb/leveldb"
	opt "github.com/syndtr/goleveldb/leveldb/opt"

	"math"

	"strconv"

	"github.com/lichuang/gpaxos/log"
)

const MINCHOSEN_KEY = math.MaxUint64
const SYSTEMVARIABLES_KEY = math.MaxUint64 - 1
const MASTERVARIABLES_KEY = math.MaxUint64 - 2

type WriteOptions struct {
}

type Database struct {
	valueStore LogStore
	hasInit    bool
	myGroupIdx int
	dbPath     string
	comparator PaxosComparator
	leveldb    *leveldb.DB
}

func (self *Database) Init(dbPath string, groupId int) error {
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

	err = self.valueStore.Init(dbPath, groupId, self)
	if err != nil {
		log.Error("value store init fail:%v", err)
		return err
	}

	self.hasInit = true
	log.Info("OK, db path:%s", dbPath)

	return nil
}

func (self *Database) GetMaxInstanceIDFileID(fileId *string, instanceId *uint64) error {
	var maxInstanceId uint64

	key := self.GenKey(maxInstanceId)
	value, err := self.leveldb.Get([]byte(key), &opt.ReadOptions{})
	if err != nil {
		return err
	}

	*fileId = string(value)
	*instanceId = maxInstanceId

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

	return fmt.Errorf("not found")
}

func (self *Database) GetInstanceIDFromKey(key string) uint64 {
	instanceId, _ := strconv.ParseUint(key, 10, 64)
	return instanceId
}

func (self *Database) RebuildOneIndex(instanceId uint64, fileIdstr string) error {
	return nil
}

func (self *Database) Get(instanceId uint64, value *string) error {
	var err error

	if !self.hasInit {
		err = fmt.Errorf("not init yet")
		return err
	}

	var fileId string
	err = self.GetFromLevelDb(instanceId, &fileId)
	if err != nil {
		return err
	}

	var fileInstanceId uint64
	err = self.FileIdToValue(fileId, &fileInstanceId, value)
	if err != nil {
		return err
	}

	if fileInstanceId != instanceId {
		return fmt.Errorf("file instance id %d not equal to instance id %d", fileInstanceId, instanceId)
	}

	return nil
}

func (self *Database) FileIdToValue(fileId string, instanceId *uint64, value *string) error {
	err := self.valueStore.Read(fileId, instanceId, value)
	if err != nil {
		log.Error("fail, ret %v", err)
		return err
	}

	return nil
}

func (self *Database) GenKey(instanceId uint64) string {
	return fmt.Sprintf("%d", instanceId)
}

func (self *Database) GetFromLevelDb(instanceId uint64, value *string) error {
	key := self.GenKey(instanceId)

	ret, err := self.leveldb.Get([]byte(key), nil)
	if err != nil {
		return err
	}

	*value = string(ret)
	return nil
}

func (self *Database) Put(options WriteOptions, instanceId uint64, value string) error {
	var err error

	if !self.hasInit {
		err = fmt.Errorf("not init yet")
		return err
	}

	var fileId string
	err = self.ValueToFileId(options, instanceId, value, &fileId)
	if err != nil {
		return err
	}

	return self.PutToLevelDB(false, instanceId, bytes.NewBufferString(fileId).Bytes())
}

func (self *Database) ValueToFileId(options WriteOptions, instanceId uint64, value string, fileId *string) error {
	err := self.valueStore.Append(options, instanceId, value, fileId)
	if err != nil {
		log.Error("append fail:%v", err)
	}
	return err
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
