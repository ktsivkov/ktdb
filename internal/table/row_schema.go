package table

import (
	"sync"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
	"ktdb/pkg/payload"
)

type RowSchema []*ColumnSchema

// LoadRow creates a row from the payload.Payload given
//
//	It relies on the RowSchema itself
func (s RowSchema) LoadRow(rowPayload payload.Payload) (*Row, error) {
	columns := make(map[string]data.Column, len(s))

	for len(rowPayload) != 0 {
		colPayload, colPayloadLen, err := rowPayload.Read()
		if err != nil {
			return nil, errors.Wrap(err, "invalid payload")
		}

		colSchema := s[len(columns)]
		var col data.Column
		if len(colPayload) != 0 {
			col, err = data.ColumnFromType(colSchema.Type, colPayload)
			if err != nil {
				return nil, errors.Wrapf(err, "could not parse column [%s]", colSchema.Name)
			}
		}

		columns[colSchema.Name] = col

		rowPayload = rowPayload[colPayloadLen:] // Subtract the payload of the column - effectively prepare for next
	}

	return &Row{
		mu:    sync.RWMutex{},
		value: columns,
	}, nil
}

// Row will create a new instance of the row based on the columns passed.
//
//	It will validate and keep only the ones specified in the schema, and ignore the others.
func (s RowSchema) Row(columns map[string]data.Column) (*Row, error) {
	val, err := s.sanitize(columns)
	if err != nil {
		return nil, err
	}

	return &Row{
		mu:    sync.RWMutex{},
		value: val,
	}, nil
}

// Find returns the *ColumnSchema by name
func (s RowSchema) Find(name string) (*ColumnSchema, error) {
	for _, col := range s {
		if col.Name == name {
			return col, nil
		}
	}

	return nil, errors.Errorf("column [%s] not found in schema", name)
}

// sanitize validates the input and pre-sets the Default value of the row, if it was not found
func (s RowSchema) sanitize(columns map[string]data.Column) (map[string]data.Column, error) {
	res := make(map[string]data.Column, len(s))
	for _, rowCol := range s {
		col, found := columns[rowCol.Name]
		if !found {
			col = rowCol.Default
		}

		if err := rowCol.Validate(col); err != nil {
			return nil, errors.Wrapf(err, "column [%s]", rowCol.Name)
		}
		res[rowCol.Name] = col
	}

	return res, nil
}
