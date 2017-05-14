package logstorage

import (
	"bytes"
	"fmt"

	leveldb "github.com/syndtr/goleveldb/leveldb"
	opt "github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/lichuang/gpaxos/log"
)

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
	return nil
}

func (self *Database) RebuildOneIndex(instanceId uint64, fileIdstr string) error {
	return nil
}

func (self *Database) Get(instanceId uint64, value string) error {
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

}

func (self *Database) FileIdToValue(fileId string, instanceId uint64, value *string) error {
	err := self.valueStore.
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
