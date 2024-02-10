package payload

import (
	"encoding/binary"

	"github.com/pkg/errors"

	"ktdb/pkg/sys"
)

type Payload []byte

func (p Payload) Size() (int, error) {
	if len(p) < sys.IntByteSize {
		return 0, errors.New("malformed payload")
	}

	if sys.IntByteSize == 2 {
		return int(binary.LittleEndian.Uint16(p[:sys.IntByteSize])), nil
	}
	if sys.IntByteSize == 4 {
		return int(binary.LittleEndian.Uint32(p[:sys.IntByteSize])), nil
	}
	if sys.IntByteSize == 8 {
		return int(binary.LittleEndian.Uint64(p[:sys.IntByteSize])), nil
	}

	return 0, errors.New("unknown int byte size detected")
}

func (p Payload) Read() ([]byte, int, error) {
	size, err := p.Size()
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to read payload size")
	}

	totalSize := sys.IntByteSize + size
	if act := len(p); act < totalSize {
		return nil, 0, errors.Errorf("expected payload with size of %d or more, got %d", totalSize, act)
	}

	return p[sys.IntByteSize:totalSize], totalSize, nil
}

func New(bytes []byte) (Payload, error) {
	size := len(bytes)
	res := make(Payload, sys.IntByteSize+size)
	copy(res[sys.IntByteSize:], bytes)

	if sys.IntByteSize == 2 {
		binary.LittleEndian.PutUint16(res, uint16(size))
		return res, nil
	}
	if sys.IntByteSize == 4 {
		binary.LittleEndian.PutUint32(res, uint32(size))
		return res, nil
	}
	if sys.IntByteSize == 8 {
		binary.LittleEndian.PutUint64(res, uint64(size))
		return res, nil
	}

	return nil, errors.New("unknown int byte size detected")
}
