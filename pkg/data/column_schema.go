package data

import (
	"unicode/utf8"

	"github.com/pkg/errors"

	"ktdb/pkg/engine"
	"ktdb/pkg/sys"
)

func LoadColumnSchemaFromBytes(processor ColumnProcessor, payload []byte) (*ColumnSchema, error) {
	schema := &ColumnSchema{}
	payloads, err := sys.ReadAll(payload)
	if err != nil {
		return nil, errors.Wrap(err, "deserialization failed")
	}
	if len(payloads) != 5 { // The payload of the ColumnSchema persists of 5 different sections, one for each field
		return nil, errors.New("corrupted payload")
	}

	if utf8.Valid(payloads[0]) == false {
		return nil, errors.Errorf("loading type failed")
	}
	schema.Type = engine.ColumnType(payloads[0])
	if utf8.Valid(payloads[2]) == false {
		return nil, errors.Errorf("loading name failed")
	}
	schema.Name = string(payloads[2])
	schema.ColumnSize, err = sys.BytesAsInt(payloads[3])
	if err != nil {
		return nil, errors.Wrap(err, "loading column size failed")
	}
	schema.Nullable, err = sys.BytesAsBool(payloads[4])
	if err != nil {
		return nil, errors.Wrap(err, "loading nullable failed")
	}
	if payloads[1][0] == 0xFF { // 0xFF is the not-nullable flag
		schema.Default, err = schema.Unmarshal(processor, payloads[1]) // // The default value should be unmarshalled last since it depends on the previous values
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling default value failed")
		}
	}

	return schema, nil
}

type ColumnSchema struct {
	Type       engine.ColumnType
	Default    engine.Column
	Name       string
	ColumnSize int
	Nullable   bool
}

func (s *ColumnSchema) Bytes() ([]byte, error) {
	typeBytes := sys.New([]byte(s.Type))
	var defaultBytes []byte
	if s.Default == nil {
		defaultBytes = make([]byte, s.ByteSize())
	} else {
		var err error
		defaultBytes, err = s.Marshal(s.Default)
		if err != nil {
			return nil, errors.Wrapf(err, "marshalling of default failed")
		}
	}
	defaultBytes = sys.New(defaultBytes)
	nameBytes := sys.New([]byte(s.Name))
	columnSizeBytes := sys.New(sys.IntAsBytes(s.ColumnSize))
	nullableByte := sys.New(sys.BoolAsBytes(s.Nullable))

	return sys.ConcatSlices(typeBytes, defaultBytes, nameBytes, columnSizeBytes, nullableByte), nil
}

func (s *ColumnSchema) ByteSize() int {
	return s.ColumnSize + 1
}

func (s *ColumnSchema) ValidateColumn(column engine.Column) error {
	if s.Nullable == false && column == nil {
		return errors.Errorf("(column=[name=%s]) is not nullable", s.Name)
	}
	if column != nil && column.Type() != s.Type {
		return errors.Errorf("(column=[name=%s]) given type [name=%s] doesn't match required type [name=%s]", s.Name, column.Type(), s.Type)
	}
	return nil
}

func (s *ColumnSchema) Marshal(column engine.Column) ([]byte, error) {
	res := make([]byte, s.ByteSize())
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

func (s *ColumnSchema) Unmarshal(processor ColumnProcessor, payload []byte) (engine.Column, error) {
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
