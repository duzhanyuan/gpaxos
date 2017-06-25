package logstorage

type LogStorage interface {
  Put(options WriteOptions, groupIdx int32, instanceId uint64, value []byte) error
  Get(groupIdx int32, instanceId uint64, value []byte) error

  // common.ErrKeyNotFound or nil
  GetMaxInstanceId(groupIdx int32, instanceId *uint64) error

  GetMinChosenInstanceId(groupIdx int32, instanceId *uint64) error
  SetMinChosenInstanceId(options WriteOptions, groupIdx int32, minInstanceId uint64) error

  SetSystemVariables(options WriteOptions, groupIdx int32, buffer []byte) error
  GetSystemVariables(groupIdx int32, buffer []byte) error

  SetMasterVariables(options WriteOptions, groupIdx int32, buffer []byte) error
  GetMasterVariables(groupIdx int32, buffer []byte) error

  Del(options WriteOptions, groupIdx int32, instanceId uint64) error

  GetLogStorageDirPath(grpupIdx int32) string

  ClearAllLog(groupIdx int32) error
}
