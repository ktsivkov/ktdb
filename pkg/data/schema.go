package data

import (
	"github.com/pkg/errors"

	"ktdb/pkg/sys"
)

func LoadRowSchemaFromBytes(processor ColumnProcessor, payload []byte) (*RowSchema, error) {
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
		rowSchema.columnSchemas[i], err = LoadColumnSchemaFromBytes(processor, columnPayloads[i+1])
		if err != nil {
			return nil, errors.Errorf("(row=[column_position=%d]) loading column schema", i)
		}
	}

	return rowSchema, nil
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

func (s *RowSchema) Prepare(columns map[string]Column) ([]Column, error) {
	res := make([]Column, len(s.columnSchemas))
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

func (s *RowSchema) Row(cols []Column) (Row, error) {
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

func (s *RowSchema) Columns(processor ColumnProcessor, row Row) ([]Column, error) {
	if rowSize := len(row); rowSize != s.rowSize {
		return nil, errors.Errorf("expected row of size [bytes=%d], got [bytes=%d]", s.rowSize, rowSize)
	}

	res := make([]Column, len(s.columnSchemas))
	startAt := 0
	endAt := 0
	for i, colSchema := range s.columnSchemas {
		endAt += colSchema.ByteSize()
		col, err := colSchema.Unmarshal(processor, row[startAt:endAt])
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
