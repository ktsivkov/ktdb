package sys

import (
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
