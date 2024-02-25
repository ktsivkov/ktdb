package engine

import (
	"unicode/utf8"

	"github.com/pkg/errors"

	"ktdb/pkg/sys"
)

type ColumnSchemaProcessor interface {
	Load(payload []byte) (*ColumnSchema, error)
}

func NewColumnSchemaProcessor(columnProcessor ColumnProcessor) (ColumnSchemaProcessor, error) {
	if columnProcessor == nil {
		return nil, errors.New("invalid value of ColumnProcessor[value=nil]")
	}

	return &columnSchemaProcessor{columnProcessor: columnProcessor}, nil
}

type columnSchemaProcessor struct {
	columnProcessor ColumnProcessor
}

func (p *columnSchemaProcessor) Load(payload []byte) (*ColumnSchema, error) {
	schema := &ColumnSchema{}
	payloads, err := sys.ReadAll(payload)
	if err != nil {
		return nil, errors.Wrap(err, "deserialization failed")
	}
	if len(payloads) != 5 { // The payload of the ColumnSchema persists of 5 different sections, one for each field
		return nil, errors.New("corrupted payload")
	}
	schema.Type, err = new(ColumnType).Load(payloads[0])
	if err != nil {
		return nil, errors.Wrap(err, "loading type failed")
	}
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
	schema.Default = payloads[1]

	return schema, nil
}
