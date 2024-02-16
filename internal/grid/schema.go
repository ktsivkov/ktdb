package grid

import (
	"github.com/pkg/errors"

	"ktdb/pkg/data"
)

type RowSchema struct {
	rowSize       int
	columnSchemas []*ColumnSchema
}

func NewRowSchema(columnSchemas []*ColumnSchema) (*RowSchema, error) {
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

		if colSchema.Type == nil {
			return nil, errors.Errorf("(row=[column_position=%d, column_name=%s]) is missing a type", i, colSchema.Name)
		}

		if colSchema.Default != nil {
			if err := colSchema.Validate(colSchema.Default); err != nil {
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

func (s *RowSchema) Prepare(columns map[string]data.Column) ([]data.Column, error) {
	res := make([]data.Column, len(s.columnSchemas))
	for i, schema := range s.columnSchemas {
		col, found := columns[schema.Name]
		if found == false {
			col = schema.Default
		}
		if err := schema.Validate(col); err != nil {
			return nil, errors.Wrapf(err, "validation failed")
		}
		res[i] = col
	}

	return res, nil
}

func (s *RowSchema) Row(cols []data.Column) (Row, error) {
	if givenCols, schemaCols := len(cols), len(s.columnSchemas); givenCols != schemaCols {
		return nil, errors.Errorf("expected columns [size=%d], got [size=%d]", givenCols, schemaCols)
	}

	row := make(Row, s.rowSize)
	byteStart := 0
	for i, col := range cols {
		schema := s.columnSchemas[i]
		bytes, err := schema.Marshal(col)
		if err != nil {
			return nil, errors.Wrap(err, "could not marshal column")
		}
		copy(row[byteStart:], bytes)
		byteStart += schema.ByteSize()
	}

	return row, nil
}

func (s *RowSchema) Columns(row Row) ([]data.Column, error) {
	if rowSize := len(row); rowSize != s.rowSize {
		return nil, errors.Errorf("expected row of size [bytes=%d], got [bytes=%d]", s.rowSize, rowSize)
	}

	res := make([]data.Column, len(s.columnSchemas))
	startAt := 0
	endAt := 0
	for i, colSchema := range s.columnSchemas {
		endAt += colSchema.ByteSize()
		col, err := colSchema.Unmarshal(row[startAt:endAt])
		if err != nil {
			return nil, errors.Wrap(err, "failed unmarshalling column")
		}
		res[i] = col
		startAt += colSchema.ByteSize()
	}

	return res, nil
}

func (s *RowSchema) ByteSize() int {
	return s.rowSize
}
