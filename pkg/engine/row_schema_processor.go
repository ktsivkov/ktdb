package engine

import (
	"github.com/pkg/errors"

	"ktdb/pkg/sys"
)

func NewRowSchemaProcessor(columnSchemaProcessor ColumnSchemaProcessor, columnProcessor ColumnProcessor) (RowSchemaProcessor, error) {
	if columnSchemaProcessor == nil {
		return nil, errors.New("invalid value of ColumnSchemaProcessor[value=nil]")
	}

	if columnProcessor == nil {
		return nil, errors.New("invalid value of ColumnProcessor[value=nil]")
	}

	return &rowSchemaProcessor{
		columnSchemaProcessor: columnSchemaProcessor,
		columnProcessor:       columnProcessor,
	}, nil
}

type RowSchemaProcessor interface {
	Load(payload []byte) (*RowSchema, error)
	New(columnSchemas []*ColumnSchema) (*RowSchema, error)
}

type rowSchemaProcessor struct {
	columnSchemaProcessor ColumnSchemaProcessor
	columnProcessor       ColumnProcessor
}

func (p *rowSchemaProcessor) Load(payload []byte) (*RowSchema, error) {
	columnPayloads, err := sys.ReadAll(payload)
	if err != nil {
		return nil, errors.Wrapf(err, "deserialization failed")
	}
	totalColumnPayloads := len(columnPayloads) - 1 // Subtract one since first element is always the row size
	if totalColumnPayloads < 0 {
		return nil, errors.New("corrupted payload")
	}
	rowSchema := &RowSchema{
		columnSchemas: make([]*ColumnSchema, totalColumnPayloads),
	}

	rowSchema.rowSize, err = sys.BytesAsInt(columnPayloads[0])
	if err != nil {
		return nil, errors.Wrap(err, "loading row size failed")
	}

	for i := range totalColumnPayloads {
		rowSchema.columnSchemas[i], err = p.columnSchemaProcessor.Load(columnPayloads[i+1])
		if err != nil {
			return nil, errors.Errorf("(row=[column_position=%d]) loading column schema", i)
		}
	}

	return rowSchema, nil
}

func (p *rowSchemaProcessor) New(columnSchemas []*ColumnSchema) (*RowSchema, error) {
	rowSize := 0
	cols := make(map[string]struct{})
	for i, colSchema := range columnSchemas {
		if colSchema == nil {
			return nil, errors.Errorf("(row=[column_position=%d]) is not defined", i)
		}

		if colSchema.Name == "" {
			return nil, errors.Errorf("(row=[column_position=%d]) is missing a name", i)
		}

		if _, found := cols[colSchema.Name]; found {
			return nil, errors.Errorf("(row=[column_position=%d, column_name=%s]) already exists", i, colSchema.Name)
		}
		cols[colSchema.Name] = struct{}{}

		if colSchema.Type == "" {
			return nil, errors.Errorf("(row=[column_position=%d, column_name=%s]) is missing a type", i, colSchema.Name)
		}

		if colSchema.Default != nil {
			col, err := colSchema.LoadColumn(p.columnProcessor, colSchema.Default)
			if err != nil {
				return nil, errors.Wrapf(err, "(row=[column_position=%d, column_name=%s]) could not load default value", i, colSchema.Name)
			}
			if err := colSchema.ValidateColumn(col); err != nil {
				return nil, errors.Wrapf(err, "(row=[column_position=%d, column_name=%s]) default value validation failed", i, colSchema.Name)
			}
		}

		rowSize += colSchema.ByteSize()
	}

	return &RowSchema{
		rowSize:       rowSize,
		columnSchemas: columnSchemas,
	}, nil
}
