package logstorage

import (
	"fmt"
	"syscall"

	"github.com/lichuang/gpaxos/common"
	"github.com/lichuang/gpaxos/log"
)

type MultiDatabase struct {
	DbList []*Database
	DbNum  int32
}

func (self *MultiDatabase) Init(dbPath string, groupCount int32) error {
	err := syscall.Access(dbPath, syscall.F_OK)
	if err != nil {
		log.Error("access dbpath %s error %v", dbPath, err)
		return err
	}

	if groupCount < 1 || groupCount > 10000 {
		err = fmt.Errorf("groupcount %d error", groupCount)
		log.Error("%v", err)

		return err
	}

	dbPath += "/"
	self.DbList = make([]*Database, groupCount)

	for i := 0; int32(i) < groupCount; i++ {
		groupDbPath := fmt.Sprintf("%sg%d", dbPath, i)
		db := new(Database)
		err = db.Init(groupDbPath, int32(i))
		if err != nil {
			return err
		}
		self.DbList[i] = db
	}

	self.DbNum = int32(len(self.DbList))

	log.Info("OK, dbpath %s groupcount %d", dbPath, groupCount)
	return nil
}

func (self *MultiDatabase) GetLogStorageDirPath(groupIdx int32) string {
	if groupIdx >= self.DbNum {
		return ""
	}

	return self.DbList[groupIdx].GetDBPath()
}

func (self *MultiDatabase) Get(groupIdx int32, instanceId uint64, value *string) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].Get(instanceId, value)
}

func (self *MultiDatabase) Put(options WriteOptions, groupIdx int32, instanceId uint64, value string) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].Put(options, instanceId, value)
}

func (self *MultiDatabase) Del(options WriteOptions, groupIdx int32, instanceId uint64) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].Del(options, instanceId)
}

func (self *MultiDatabase) ForceDel(options WriteOptions, groupIdx int32, instanceId uint64) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].ForceDel(options, instanceId)
}

func (self *MultiDatabase) SetMinChosenInstanceID(options WriteOptions, groupIdx int32, minInstanceId uint64) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].SetMinChosenInstanceID(options, minInstanceId)
}

func (self *MultiDatabase) GetMinChosenInstanceID(groupIdx int32, minInstanceId *uint64) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].GetMinChosenInstanceID(minInstanceId)
}

func (self *MultiDatabase) ClearAllLog(groupIdx int32) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].ClearAllLog()
}

func (self *MultiDatabase) SetSystemVariables(options WriteOptions, groupIdx int32, buffer []byte) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].SetSystemVariables(options, buffer)
}

func (self *MultiDatabase) GetSystemVariables(groupIdx int32, buffer *[]byte) error {
	if groupIdx >= self.DbNum {
		return common.ErrInvalidGroupIndex
	}

	return self.DbList[groupIdx].GetSystemVariables(buffer)
}
