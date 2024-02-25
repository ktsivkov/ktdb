package engine

import (
	"github.com/pkg/errors"

	"ktdb/pkg/sys"
)

type ColumnSchema struct {
	Type       ColumnType
	Default    []byte
	Name       string
	ColumnSize int
	Nullable   bool
}

func (s *ColumnSchema) Bytes() ([]byte, error) {
	typeBytes := sys.New(s.Type.Bytes())
	defaultBytes := sys.New(s.Default)
	nameBytes := sys.New([]byte(s.Name))
	columnSizeBytes := sys.New(sys.IntAsBytes(s.ColumnSize))
	nullableByte := sys.New(sys.BoolAsBytes(s.Nullable))

	return sys.ConcatSlices(typeBytes, defaultBytes, nameBytes, columnSizeBytes, nullableByte), nil
}

func (s *ColumnSchema) ValidateColumn(column Column) error {
	if s.Nullable == false && column == nil {
		return errors.Errorf("(column=[name=%s]) is not nullable", s.Name)
	}
	if column != nil && column.Type() != s.Type {
		return errors.Errorf("(column=[name=%s]) given type [name=%s] doesn't match required type [name=%s]", s.Name, column.Type(), s.Type)
	}
	return nil
}

func (s *ColumnSchema) ColumnBytes(column Column) ([]byte, error) {
	// TODO: check how to avoid the +1 on non-nullable fields without ruining the validation
	res := make([]byte, s.ByteSize()) // The data size is always one byte larger, accommodating for nullable flag
	if err := s.ValidateColumn(column); err != nil {
		return nil, errors.Wrapf(err, "column validation failed")
	}

	if column == nil && s.Nullable {
		return res, nil
	}
	res[0] = 0xFF // Indicates the column is not null
	bytes, err := column.Bytes(s.ColumnSize)
	if err != nil {
		return nil, errors.Wrapf(err, "(column=[name=%s]) marshal failed", s.Name)
	}

	copy(res[1:], bytes)
	return res, nil
}

func (s *ColumnSchema) LoadColumn(processor ColumnProcessor, payload []byte) (Column, error) {
	res, err := processor.FromType(s.Type, s.ColumnSize, payload) // Skip first byte since it is just a nullable flag
	if err != nil {
		return nil, errors.Wrapf(err, "(column=[name=%s]) unmarshal failed", s.Name)
	}

	return res, nil
}

func (s *ColumnSchema) LoadColumnFromPayload(processor ColumnProcessor, payload []byte) (Column, error) {
	if size, expected := len(payload), s.ByteSize(); size != expected {
		return nil, errors.Errorf("(column=[name=%s]) corrupted data, payload size [size=%d] differs than the expected [size=%d]", s.Name, size, expected)
	}

	if payload[0] == 0x00 {
		if s.Nullable {
			return nil, nil
		}
		return nil, errors.Errorf("(column=[name=%s]) corrupted data, cannot assign null on a not-nullable column", s.Name)
	}

	res, err := processor.FromType(s.Type, s.ColumnSize, payload[1:]) // Skip first byte since it is just a nullable flag
	if err != nil {
		return nil, errors.Wrapf(err, "(column=[name=%s]) unmarshal failed", s.Name)
	}

	return res, nil
}

func (s *ColumnSchema) ByteSize() int {
	return s.ColumnSize + 1
}
