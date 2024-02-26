package row

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine/grid/column"
	"ktdb/pkg/sys"
)

type Schema struct {
	columnSchemas []*column.Schema
	rowSize       int64
}

func (s *Schema) Bytes() ([]byte, error) {
	colSchemaBytes := make([][]byte, len(s.columnSchemas)+1)
	colSchemaBytes[0] = sys.New(sys.Int64AsBytes(s.rowSize))
	for i, colSchema := range s.columnSchemas {
		colBytes, err := colSchema.Bytes()
		if err != nil {
			return nil, errors.Wrapf(err, "(row=[column_position=%d]) could not get bytes of the schema", i)
		}
		colSchemaBytes[i+1] = sys.New(colBytes)
	}

	return sys.ConcatSlices(colSchemaBytes...), nil
}

func (s *Schema) Load(payload []byte) error {
	columnPayloads, err := sys.ReadAll(payload)
	if err != nil {
		return errors.Wrapf(err, "deserialization failed")
	}
	totalColumnPayloads := len(columnPayloads) - 1 // Subtract one since first element is always the row size
	if totalColumnPayloads < 0 {
		return errors.New("corrupted payload")
	}

	s.columnSchemas = make([]*column.Schema, totalColumnPayloads)
	s.rowSize, err = sys.BytesAsInt64(columnPayloads[0])
	if err != nil {
		return errors.Wrap(err, "loading row size failed")
	}

	for i := range totalColumnPayloads {
		colSchema := &column.Schema{}
		if err := colSchema.Load(columnPayloads[i+1]); err != nil {
			return errors.Errorf("(row=[column_position=%d]) loading column schema", i)
		}
		s.columnSchemas[i] = colSchema
	}

	return nil
}

func (s *Schema) Row(cols []column.Column) (Row, error) {
	if givenCols, schemaCols := len(cols), len(s.columnSchemas); givenCols != schemaCols {
		return nil, errors.Errorf("expected columns [size=%d], got [size=%d]", givenCols, schemaCols)
	}

	res := make(Row, s.rowSize)
	byteStart := int64(0)
	for i, col := range cols {
		colSchema := s.columnSchemas[i]
		bytes, err := colSchema.ColumnBytes(col)
		if err != nil {
			return nil, errors.Wrap(err, "could not marshal column")
		}
		copy(res[byteStart:], bytes)
		byteStart += colSchema.PayloadSize()
	}

	return res, nil
}

func (s *Schema) Columns(processor column.Processor, row Row) ([]column.Column, error) {
	if rowSize := int64(len(row)); rowSize != s.rowSize {
		return nil, errors.Errorf("expected row of size [bytes=%d], got [bytes=%d]", s.rowSize, rowSize)
	}

	res := make([]column.Column, len(s.columnSchemas))
	startAt := int64(0)
	endAt := int64(0)
	for i, colSchema := range s.columnSchemas {
		endAt += colSchema.PayloadSize()
		col, err := colSchema.Column(processor, row[startAt:endAt])
		if err != nil {
			return nil, errors.Wrap(err, "failed unmarshalling column")
		}
		res[i] = col
		startAt += colSchema.PayloadSize()
	}

	return res, nil
}

func (s *Schema) ByteSize() int64 {
	return s.rowSize
}
