package payload_test

import (
	"encoding/binary"

	"ktdb/pkg/sys"
)

func IntAsBytes(i int) []byte {
	res := make([]byte, sys.IntByteSize)
	if sys.IntByteSize == 2 {
		binary.LittleEndian.PutUint16(res, uint16(i))
	}
	if sys.IntByteSize == 4 {
		binary.LittleEndian.PutUint32(res, uint32(i))
	}
	if sys.IntByteSize == 8 {
		binary.LittleEndian.PutUint64(res, uint64(i))
	}

	return res
}

func ConcatSlices[T any](slices ...[]T) []T {
	var totalLen int

	for _, s := range slices {
		totalLen += len(s)
	}

	result := make([]T, totalLen)

	var i int
	for _, s := range slices {
		i += copy(result[i:], s)
	}

	return result
}
