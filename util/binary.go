package util

import (
    "bytes"
	"encoding/binary"
)

func DecodeUint64(buffer []byte, offset int, ret *uint64) {
	*ret = binary.LittleEndian.Uint64(buffer[offset:])
}

func EncodeUint64(buffer []byte, offset int, ret uint64) {
	binary.LittleEndian.PutUint64(buffer[offset:], ret)
}

func DecodeInt32(buffer []byte, offset int, ret *int32) {
	tmp := binary.LittleEndian.Uint32(buffer[offset:])
	*ret = int32(tmp)
}

func EncodeInt32(buffer []byte, offset int, ret int32) {
	binary.LittleEndian.PutUint32(buffer[offset:], uint32(ret))
}

func DecodeUint32(buffer []byte, offset int, ret *uint32) {
	*ret = binary.LittleEndian.Uint32(buffer[offset:])
}

func EncodeUint32(buffer []byte, offset int, ret uint32) {
	binary.LittleEndian.PutUint32(buffer[offset:], ret)
}

func DecodeUint16(buffer []byte, offset int, ret *uint16) {
    *ret = binary.LittleEndian.Uint16(buffer[offset:])
}

func EncodeUint16(buffer []byte, offset int, ret uint16) {
    binary.LittleEndian.PutUint16(buffer[offset:], ret)
}

func AppendBytes(inputs ...[]byte)[] byte {
    return bytes.Join(inputs, []byte(""))
}