package grid

import (
	"reflect"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
)

type ColumnSchema struct {
	Name       string
	ColumnSize int
	Nullable   bool
	Default    data.Column
	Type       reflect.Type
}

func (s *ColumnSchema) ByteSize() int {
	return s.ColumnSize + 1
}

func (s *ColumnSchema) Validate(column data.Column) error {
	if s.Nullable == false && column == nil {
		return errors.Errorf("(column=[name=%s]) is not nullable", s.Name)
	}

	if colType := reflect.TypeOf(column); column != nil && colType != s.Type {
		wantedTypeName := reflect.New(s.Type).Interface().(data.Column).TypeName(s.ColumnSize)
		return errors.Errorf("(column=[name=%s]) given type [name=%s, type=%s] doesn't match required type [name=%s, type=%s]", s.Name, column.TypeName(s.ColumnSize), colType.String(), wantedTypeName, s.Type.String())
	}

	return nil
}

func (s *ColumnSchema) Marshal(column data.Column) ([]byte, error) {
	res := make([]byte, s.ByteSize())
	if err := s.Validate(column); err != nil {
		return nil, errors.Wrapf(err, "validation failed")
	}
	if column == nil && s.Nullable {
		return res, nil
	}
	res[0] = 0xFF // Indicates the column is not null
	bytes, err := column.Marshal(s.ColumnSize)
	if err != nil {
		return nil, errors.Wrapf(err, "(column=[name=%s]) marshal failed", s.Name)
	}
	copy(res[1:], bytes)
	return res, nil
}

func (s *ColumnSchema) Unmarshal(payload []byte) (data.Column, error) {
	if size, expected := len(payload), s.ByteSize(); size != expected {
		return nil, errors.Errorf("(column=[name=%s]) corrupted data, payload size [size=%d] differs than the expected [size=%d]", s.Name, size, expected)
	}

	if payload[0] == 0x00 {
		if s.Nullable {
			return nil, nil
		}
		return nil, errors.Errorf("(column=[name=%s]) corrupted data, cannot assign null on a not-nullable column", s.Name)
	}

	res, err := data.ColumnFromType(s.Type, s.ColumnSize, payload[1:]) // Skip first byte since it is just a nullable flag
	if err != nil {
		return nil, errors.Wrapf(err, "(column=[name=%s]) unmarshal failed", s.Name)
	}

	return res, nil
}
