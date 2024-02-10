package table

import (
	"sync"

	"github.com/pkg/errors"

	"ktdb/pkg/payload"
)

type Table struct {
	Name      string
	RowSchema RowSchema
	mu        sync.RWMutex
	Rows      []*Row
}

func (t *Table) Bytes() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	bytes := make([]byte, 0)
	for i, row := range t.Rows {
		rowBytes, err := row.Bytes(t.RowSchema)
		if err != nil {
			return nil, errors.Wrapf(err, "could not get bytes of row [%d]", i)
		}

		rowPayload, err := payload.New(rowBytes)
		if err != nil {
			return nil, errors.Wrapf(err, "could not create bytes payload of row [%d]", i)
		}
		bytes = append(bytes, rowPayload...)
	}

	return payload.New(bytes)
}

func (t *Table) Append(row *Row) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Rows = append(t.Rows, row)
}

func (t *Table) Get(i int) (*Row, error) {
	if i > len(t.Rows) {
		return nil, errors.Errorf("row [%d] does not exist", i)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Rows[i], nil
}

func (t *Table) Update(i int, row *Row) error {
	if i > len(t.Rows) {
		return errors.Errorf("row [%d] does not exist", i)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.Rows[i] = row
	return nil
}

func (t *Table) Load(tablePayload payload.Payload) error {
	rowsPayload, _, err := tablePayload.Read()
	if err != nil {
		return errors.Wrapf(err, "invalid payload for table [%s]", t.Name)
	}

	rows := make([]*Row, 0)
	for len(rowsPayload) != 0 {
		rowPayload, rowPayloadLen, err := payload.Payload(rowsPayload).Read()
		if err != nil {
			return errors.Wrap(err, "invalid payload")
		}

		row, err := t.RowSchema.LoadRow(rowPayload)
		if err != nil {
			return errors.Wrapf(err, "invalid row bytes")
		}
		rows = append(rows, row)

		rowsPayload = rowsPayload[rowPayloadLen:] // Subtract the payload of the row - effectively prepare for next
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.Rows = rows

	return nil
}
