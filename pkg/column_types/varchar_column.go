package column_types

import (
	"unicode/utf8"

	"github.com/pkg/errors"

	"ktdb/pkg/engine/grid/column"
	"ktdb/pkg/sys"
)

const TypeVarchar column.Type = "varchar"

type VarcharProcessor struct{}

func (v *VarcharProcessor) Type() column.Type {
	return TypeVarchar
}

func (v *VarcharProcessor) Load(size int64, payload []byte) (column.Column, error) {
	if ps := int64(len(payload)); ps != size {
		return nil, errors.Errorf("(%s) payload byte size [size=%d] exceeds allocated size", v.Type().Format(size), ps)
	}

	if utf8.Valid(payload) == false {
		return nil, errors.Errorf("(%s) payload bytes are not valid UTF-8", v.Type().Format(size))
	}

	return Varchar(sys.RemovePadding(payload)), nil
}

type Varchar string

func (v Varchar) Type() column.Type {
	return TypeVarchar
}

func (v Varchar) Bytes(size int64) ([]byte, error) {
	payload := []byte(v)
	if int64(len(payload)) > size {
		return nil, errors.Errorf("(%s) data exceeds maximum size", v.Type().Format(size))
	}

	return sys.AddPadding(payload, size)
}
