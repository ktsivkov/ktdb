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

func (v *VarcharProcessor) Load(size int, payload []byte) (column.Column, error) {
	if utf8.Valid(payload) == false {
		return nil, errors.Errorf("(%s) payload bytes are not valid UTF-8", v.Type().Format(size))
	}

	return Varchar(sys.RemovePadding(payload)), nil
}

type Varchar string

func (v Varchar) Type() column.Type {
	return TypeVarchar
}

func (v Varchar) Bytes(size int) ([]byte, error) {
	payload := []byte(v)
	if len(payload) > size {
		return nil, errors.Errorf("(%s) data exceeds maximum size", v.Type().Format(size))
	}

	return sys.AddPadding(payload, size)
}
