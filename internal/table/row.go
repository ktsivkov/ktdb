package table

import (
	"sync"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
	"ktdb/pkg/payload"
)

type Row struct {
	mu    sync.RWMutex
	value map[string]data.Column
}

func (r *Row) Set(schema RowSchema, name string, col data.Column) error {
	colSchema, err := schema.Find(name)
	if err != nil {
		return err
	}

	if err := colSchema.Validate(col); err != nil {
		return errors.Wrapf(err, "column [%s]", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.value[name] = col
	return nil
}

func (r *Row) Get(name string) (data.Column, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	val, found := r.value[name]
	if found == false {
		return nil, errors.Errorf("column [%s] doesn't exist", name)
	}

	return val, nil
}

func (r *Row) Bytes(schema RowSchema) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	res := make([]byte, 0)
	for _, colSchema := range schema {
		col := r.value[colSchema.Name]
		var colBytes []byte
		if col != nil {
			var err error
			colBytes, err = col.Marshal()
			if err != nil {
				return nil, errors.Wrapf(err, "could not marshal column [%s]", colSchema.Name)
			}
		}

		colPayload, err := payload.New(colBytes)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create bytes payload of column [%s]", colSchema.Name)
		}

		res = append(res, colPayload...)
	}

	return res, nil
}
