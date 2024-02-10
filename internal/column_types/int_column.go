package column_types

import (
	"encoding/binary"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
	"ktdb/pkg/payload"
	"ktdb/pkg/sys"
)

// Int is a structure that is to represent column type Int, the size of the payload is based on the system architecture.
// Supported architectures of int size 16, 32, 64
type Int int

func (i Int) TypeName() string {
	return "int"
}

func (i Int) Marshal() (payload.Payload, error) {
	res := make(payload.Payload, sys.IntByteSize)
	if sys.IntByteSize == 2 {
		binary.LittleEndian.PutUint16(res, uint16(i))
		return payload.New(res)
	}

	if sys.IntByteSize == 4 {
		binary.LittleEndian.PutUint32(res, uint32(i))
		return payload.New(res)
	}

	if sys.IntByteSize == 8 {
		binary.LittleEndian.PutUint64(res, uint64(i))
		return payload.New(res)
	}

	return nil, errors.Errorf("unknown int byte size (%d) detected", sys.IntByteSize)
}

func (i Int) Unmarshal(payload payload.Payload) (data.Column, error) {
	var err error
	payload, _, err = payload.Read()
	if err != nil {
		return nil, errors.Wrapf(err, "(%s) could not read payload", i.TypeName())
	}

	if ps := len(payload); ps != sys.IntByteSize {
		return nil, errors.Errorf("(%s) payload byte count mismatch", i.TypeName())
	}

	if sys.IntByteSize == 2 {
		return Int(binary.LittleEndian.Uint16(payload)), nil
	}

	if sys.IntByteSize == 4 {
		return Int(binary.LittleEndian.Uint32(payload)), nil
	}

	if sys.IntByteSize == 8 {
		return Int(binary.LittleEndian.Uint64(payload)), nil
	}

	return nil, errors.Errorf("(%s) unknown int byte size detected", i.TypeName())
}
