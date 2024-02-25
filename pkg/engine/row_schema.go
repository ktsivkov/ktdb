package engine

import (
	"github.com/pkg/errors"

	"ktdb/pkg/sys"
)

type RowSchema struct {
	columnSchemas []*ColumnSchema
	rowSize       int
}

func (s *RowSchema) Bytes() ([]byte, error) {
	colSchemaBytes := make([][]byte, len(s.columnSchemas)+1)
	colSchemaBytes[0] = sys.New(sys.IntAsBytes(s.rowSize))
	for i, colSchema := range s.columnSchemas {
		colBytes, err := colSchema.Bytes()
		if err != nil {
			return nil, errors.Wrapf(err, "(row=[column_position=%d, column_name=%s]) could not get bytes of the schema", i, colSchema.Name)
		}
		colSchemaBytes[i+1] = sys.New(colBytes)
	}

	return sys.ConcatSlices(colSchemaBytes...), nil
}

func (s *RowSchema) Prepare(columnProcessor ColumnProcessor, columns map[string]Column) ([]Column, error) {
	res := make([]Column, len(s.columnSchemas))
	for i, colSchema := range s.columnSchemas {
		col, found := columns[colSchema.Name]
		if found == false && colSchema.Default != nil {
			var err error
			col, err = colSchema.LoadColumn(columnProcessor, colSchema.Default)
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

func (s *RowSchema) Row(cols []Column) (Row, error) {
	if givenCols, schemaCols := len(cols), len(s.columnSchemas); givenCols != schemaCols {
		return nil, errors.Errorf("expected columns [size=%d], got [size=%d]", givenCols, schemaCols)
	}

	row := make(Row, s.rowSize)
	byteStart := 0
	for i, col := range cols {
		colSchema := s.columnSchemas[i]
		bytes, err := colSchema.ColumnBytes(col)
		if err != nil {
			return nil, errors.Wrap(err, "could not marshal column")
		}
		copy(row[byteStart:], bytes)
		byteStart += colSchema.ByteSize()
	}

	return row, nil
}

func (s *RowSchema) Columns(processor ColumnProcessor, row Row) ([]Column, error) {
	if rowSize := len(row); rowSize != s.rowSize {
		return nil, errors.Errorf("expected row of size [bytes=%d], got [bytes=%d]", s.rowSize, rowSize)
	}

	res := make([]Column, len(s.columnSchemas))
	startAt := 0
	endAt := 0
	for i, colSchema := range s.columnSchemas {
		endAt += colSchema.ByteSize()
		col, err := colSchema.LoadColumnFromPayload(processor, row[startAt:endAt])
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
