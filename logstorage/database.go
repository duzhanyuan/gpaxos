package logstorage

type Database struct {
}

func (self *Database) GetMaxInstanceIDFileID(fileId *string, instanceId *uint64) error {
	return nil
}

func (self *Database) RebuildOneIndex(instanceId uint64, fileIdstr string) error {
	return nil
}
