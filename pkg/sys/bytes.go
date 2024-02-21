package sys

import (
	"encoding/binary"
	"unsafe"

	"github.com/pkg/errors"
)

// IntByteSize represents the size of a typical int in bytes depending on the system's architecture.
const IntByteSize = int(unsafe.Sizeof(0))

func AddPadding(data []byte, desiredSize int) ([]byte, error) {
	if size := len(data); size > desiredSize {
		return nil, errors.Errorf("data size [size=%d] exceeds desired size [desired_size=%d]", size, desiredSize)
	}
	res := make([]byte, desiredSize)
	copy(res, data)
	return res, nil
}

func RemovePadding(data []byte) []byte {
	endIndex := len(data)
	for endIndex > 0 && data[endIndex-1] == 0x00 {
		endIndex--
	}

	return data[:endIndex]
}

func IntAsBytes(i int) []byte {
	res := make([]byte, IntByteSize)
	switch IntByteSize {
	case 2:
		binary.LittleEndian.PutUint16(res, uint16(i))
	case 4:
		binary.LittleEndian.PutUint32(res, uint32(i))
	case 8:
		binary.LittleEndian.PutUint64(res, uint64(i))
	default:
		panic("unknown system architecture")
	}
	return res
}

func BytesAsInt(bytes []byte) (int, error) {
	if len(bytes) != IntByteSize {
		return 0, errors.New("unsupported int bytes size")
	}
	switch IntByteSize {
	case 2:
		return int(binary.LittleEndian.Uint16(bytes)), nil
	case 4:
		return int(binary.LittleEndian.Uint32(bytes)), nil
	case 8:
		return int(binary.LittleEndian.Uint64(bytes)), nil
	default:
		return 0, errors.New("unsupported system int bytes size")
	}
}

func BoolAsBytes(val bool) []byte {
	if val {
		return []byte{0x01}
	}
	return []byte{0x00}
}

func BytesAsBool(val []byte) (bool, error) {
	if len(val) != 1 {
		return false, errors.New("corrupted data, expected 1 byte")
	}
	switch val[0] {
	case 0x00:
		return false, nil
	case 0x01:
		return true, nil
	default:
		return false, errors.New("corrupted data, unknown value")
	}
}
