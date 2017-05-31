package logstorage

import (
	proto "github.com/golang/protobuf/proto"
	"github.com/lichuang/gpaxos/common"
	"github.com/lichuang/gpaxos/log"
)

type SystemVariablesStore struct {
	LogStorage LogStorage
}

func (self *SystemVariablesStore) Write(options WriteOptions, groupIdx int32, variables *common.SystemVariables) error {
	buf, err := proto.Marshal(variables)
	if err != nil {
		log.Error("Variables.Serialize fail:%v", err)
		return err
	}

	err = self.LogStorage.SetSystemVariables(options, groupIdx, buf)
	if err != nil {
		log.Error("db.put fail, groupidx %d bufferlen %d err:%v", groupIdx, len(buf), err)
		return err

	}

	return nil
}

func (self *SystemVariablesStore) Read(groupIdx int32, variables *common.SystemVariables) error {
	var buffer []byte
	err := self.LogStorage.GetSystemVariables(groupIdx, &buffer)
	if err != nil {
		log.Error("db.get fail, groupidx %d err:%v", groupIdx, err)
		return err

	}

	err = proto.Unmarshal(buffer, variables)
	if err != nil {
		log.Error("Variables.ParseFromArray fail, bufferlen %d", len(buffer))
		return err
	}
	return nil
}
