package column_types

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
)

// Int is a structure that is to represent column type Int, the size of the payload is based on the system architecture.
// Supported architectures of int size 16, 32, 64 bit size
type Int int

func (i Int) Identifier() string {
	return "int"
}

func (i Int) Type(size int) string {
	return fmt.Sprintf("%s[size=%d]", i.Identifier(), size)
}

func (i Int) Marshal(size int) ([]byte, error) {
	res := make([]byte, size)
	if size == 2 {
		if i < math.MinInt16 || i > math.MaxInt16 {
			return nil, errors.Errorf("(%s) number [int=%d] out of range", i.Type(size), i)
		}
		binary.LittleEndian.PutUint16(res, uint16(i))
		return res, nil
	}

	if size == 4 {
		if i < math.MinInt32 || i > math.MaxInt32 {
			return nil, errors.Errorf("(%s) number [int=%d] out of range", i.Type(size), i)
		}
		binary.LittleEndian.PutUint32(res, uint32(i))
		return res, nil
	}

	if size == 8 {
		if i < math.MinInt64 || i > math.MaxInt64 {
			return nil, errors.Errorf("(%s) number [int=%d] out of range", i.Type(size), i)
		}
		binary.LittleEndian.PutUint64(res, uint64(i))
		return res, nil
	}

	return nil, errors.Errorf("(%s) unsupported size", i.Type(size))
}

func (i Int) Unmarshal(size int, payload []byte) (data.Column, error) {
	if ps := len(payload); ps != size {
		return nil, errors.Errorf("(%s) payload byte size [size=%d] exceeds allocated size", i.Type(size), ps)
	}

	if size == 2 {
		return Int(binary.LittleEndian.Uint16(payload)), nil
	}

	if size == 4 {
		return Int(binary.LittleEndian.Uint32(payload)), nil
	}

	if size == 8 {
		return Int(binary.LittleEndian.Uint64(payload)), nil
	}

	return nil, errors.Errorf("(%s) unsupported size", i.Type(size))
}
