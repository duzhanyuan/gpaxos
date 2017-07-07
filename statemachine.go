package gpaxos

type StateMachineContext struct {
  SMId int32
  Context interface{}
}

func NewStateMachineContext(smid int32, context interface{}) *StateMachineContext {
  return &StateMachineContext{
    SMId:smid,
    Context:context,
  }
}

type BatchStateMachineContext struct {
  StateMachineContexts []*StateMachineContext
}

func NewBatchStateMachineContext() *BatchStateMachineContext {
  return &BatchStateMachineContext{
    StateMachineContexts:make([]*StateMachineContext, 0),
  }
}

type StateMachine interface {
  Execute(groupIdx int32, instanceId uint64, paxosValue[]byte, context *StateMachineContext) bool

  SMID() int32

  ExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue []byte)bool

  GetCheckpointInstanceID(groupIdx int32) uint64

  LockCheckpointState() error

  GetCheckpointState(groupIdx int32, dirPath *string, fileList []string) error

  UnLockCheckpointState()

  LoadCheckpointState(groupIdx int32, checkpointTmpFileDirPath string,
                      fileList []string, checkpointInstanceID uint64) error
}
