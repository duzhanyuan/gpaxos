package util

import (
	"hash/crc32"
)

func Crc32(crc uint32, value []byte, skiplen int) uint32 {
	vlen := len(value)
	data := value[:vlen-skiplen]
	return crc32.Update(crc, crc32.IEEETable, []byte(data))
}
