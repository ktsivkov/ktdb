package column_types

import (
	"encoding/binary"
	"math"

	"github.com/pkg/errors"

	"ktdb/pkg/engine"
)

const TypeInt engine.ColumnType = "int"

type IntProcessor struct{}

func (i *IntProcessor) Type() engine.ColumnType {
	return TypeInt
}

func (i *IntProcessor) Load(size int, payload []byte) (engine.Column, error) {
	if ps := len(payload); ps != size {
		return nil, errors.Errorf("(%s) payload byte size [size=%d] exceeds allocated size", i.Type().Format(size), ps)
	}

	var res Int
	switch size {
	case 2:
		res = Int(binary.LittleEndian.Uint16(payload))
	case 4:
		res = Int(binary.LittleEndian.Uint32(payload))
	case 8:
		res = Int(binary.LittleEndian.Uint64(payload))
	default:
		return nil, errors.Errorf("(%s) unsupported size", i.Type().Format(size))
	}
	return res, nil
}

// Int is a structure that is to represent column type Int, the size of the payload is based on the system architecture.
// Supported architectures of int size 16, 32, 64 bit size
type Int int

func (i Int) Type() engine.ColumnType {
	return TypeInt
}

func (i Int) Bytes(size int) ([]byte, error) {
	res := make([]byte, size)
	switch size {
	case 2:
		if i < math.MinInt16 || i > math.MaxInt16 {
			return nil, errors.Errorf("(%s) number [int=%d] out of range", i.Type().Format(size), i)
		}
		binary.LittleEndian.PutUint16(res, uint16(i))
		return res, nil
	case 4:
		if i < math.MinInt32 || i > math.MaxInt32 {
			return nil, errors.Errorf("(%s) number [int=%d] out of range", i.Type().Format(size), i)
		}
		binary.LittleEndian.PutUint32(res, uint32(i))
		return res, nil
	case 8:
		if i < math.MinInt64 || i > math.MaxInt64 {
			return nil, errors.Errorf("(%s) number [int=%d] out of range", i.Type().Format(size), i)
		}
		binary.LittleEndian.PutUint64(res, uint64(i))
		return res, nil
	default:
		return nil, errors.Errorf("(%s) unsupported size", i.Type().Format(size))
	}
}
