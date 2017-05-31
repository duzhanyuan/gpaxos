package logstorage

type LogStorage interface {
	Put(options WriteOptions, groupIdx int32, instanceId uint64, value []byte) error
	Get(groupIdx int32, instanceId uint64, value *[]byte) error
	GetMaxInstanceId(groupIdx int32, instanceId *uint64) error
	SetSystemVariables(options WriteOptions, groupIdx int32, buffer []byte) error
	GetSystemVariables(groupIdx int32, buffer *[]byte) error
}
