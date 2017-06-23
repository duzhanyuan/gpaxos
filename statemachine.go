package gpaxos

type StateMachine interface {
  Execute(groupIdx int32, instanceId uint64, paxosValue[]byte, context *StateMachineContext) bool

  SMID() int32

  ExecuteForCheckpoint(groupIdx int32, instanceId uint64, paxosValue []byte)bool

  GetCheckpointInstanceID(groupIdx int32) uint64

  LockCheckpointState() int

  GetCheckpointState(groupIdx int32, dirPath string, fileList []string) int

  UnLockCheckpointState()

  LoadCheckpointState(groupIdx int32, checkpointTmpFileDirPath string,
                      fileList []string, checkpointInstanceID uint64) int
}
