package row

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine/grid/column"
)

type Processor interface {
	Prepare(schema *Schema, columns map[string]column.Column) ([]column.Column, error)
	New(columnSchemas []*column.Schema) (*Schema, error)
}

func NewProcessor(columnProcessor column.Processor) (Processor, error) {
	if columnProcessor == nil {
		return nil, errors.New("invalid value of ColumnProcessor[value=nil]")
	}

	return &processor{
		columnProcessor: columnProcessor,
	}, nil
}

type processor struct {
	columnProcessor column.Processor
}

func (p *processor) Prepare(schema *Schema, columns map[string]column.Column) ([]column.Column, error) {
	res := make([]column.Column, len(schema.columnSchemas))
	for i, colSchema := range schema.columnSchemas {
		col, found := columns[colSchema.Name]
		if found == false && colSchema.Default != nil {
			var err error
			col, err = colSchema.Column(p.columnProcessor, colSchema.Default)
			if err != nil {
				return nil, errors.Wrap(err, "unable to load default value")
			}
		}

		if err := colSchema.ValidateColumn(col); err != nil {
			return nil, errors.Wrap(err, "validation failed")
		}
		res[i] = col
	}

	return res, nil
}

func (p *processor) New(columnSchemas []*column.Schema) (*Schema, error) {
	rowSize := int64(0)
	cols := make(map[string]struct{})
	for i, colSchema := range columnSchemas {
		if colSchema == nil {
			return nil, errors.Errorf("(row=[column_position=%d]) is not defined", i)
		}
		if _, found := cols[colSchema.Name]; found {
			return nil, errors.Errorf("(row=[column_position=%d, column_name=%s]) already exists", i, colSchema.Name)
		}
		cols[colSchema.Name] = struct{}{}
		rowSize += colSchema.PayloadSize()
	}

	return &Schema{
		rowSize:       rowSize,
		columnSchemas: columnSchemas,
	}, nil
}
