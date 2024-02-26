package table

import (
	"fmt"

	"github.com/pkg/errors"

	"ktdb/pkg/engine/grid/row"
	"ktdb/pkg/storage"
)

type Table interface {
	Schema() *row.Schema
	Row(id int64) (row.Row, error)
	Set(id int64, r row.Row) error
	Append(r row.Row) error
	TotalRows() (int64, error)
}

type table struct {
	reader storage.Reader
	writer storage.Writer
	// schema is set by load
	schema *row.Schema
	name   string
}

func (t *table) TotalRows() (int64, error) {
	info, err := t.reader.Info(t.dataFile())
	if err != nil {
		return 0, errors.Wrapf(err, "%s could not read data file info", t.logDescriptor())
	}

	return info.Size() / t.schema.ByteSize(), nil
}

func (t *table) Schema() *row.Schema {
	return t.schema
}

func (t *table) Row(id int64) (row.Row, error) {
	if id < 1 {
		return nil, errors.Errorf("%s invalid row=%s", t.logDescriptor(), t.rowLogDescriptor(id))
	}

	rowBytes, err := t.reader.ReadPartials(t.dataFile(), []*storage.Partial{
		{
			OffsetFrom: t.schema.ByteSize() * (id - 1),
			OffsetTo:   t.schema.ByteSize() * id,
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "could not read row=%s", t.logDescriptor())
	}

	return rowBytes[0], nil
}

func (t *table) Set(id int64, r row.Row) error {
	if id < 1 {
		return errors.Errorf("%s invalid row=%s", t.logDescriptor(), t.rowLogDescriptor(id))
	}
	if err := t.writer.Offset(t.dataFile(), t.schema.ByteSize()*(id-1), r); err != nil {
		return errors.Wrapf(err, "%s could not set row=%s", t.logDescriptor(), t.rowLogDescriptor(id))
	}
	return nil
}

func (t *table) Append(r row.Row) error {
	if err := t.writer.Append(t.dataFile(), r); err != nil {
		return errors.Wrapf(err, "%s could not append row", t.logDescriptor())
	}
	return nil
}

func (t *table) schemaFile() string {
	return fmt.Sprintf("%s.schema", t.name)
}

func (t *table) dataFile() string {
	return fmt.Sprintf("%s.data", t.name)
}

func (t *table) load() error {
	schemaPayload, err := t.reader.ReadAll(t.schemaFile())
	if err != nil {
		return errors.Wrapf(err, "%s could not read schema", t.logDescriptor())
	}

	t.schema = &row.Schema{}
	if err := t.schema.Load(schemaPayload); err != nil {
		return errors.Wrapf(err, "%s could not load schema", t.logDescriptor())
	}

	return nil
}

func (t *table) create() error {
	schemaPayload, err := t.schema.Bytes()
	if err != nil {
		return errors.Wrapf(err, "%s could not get schema bytes", t.logDescriptor())
	}
	if err := t.writer.CreateOrOverride(t.schemaFile(), schemaPayload); err != nil {
		return errors.Wrapf(err, "%s could not create schema file", t.logDescriptor())
	}
	if err := t.writer.CreateOrOverride(t.dataFile(), nil); err != nil {
		return errors.Wrapf(err, "%s could not create data file", t.logDescriptor())
	}
	return nil
}

func (t *table) rowLogDescriptor(id int64) string {
	return fmt.Sprintf("[id=%d]", id)
}

func (t *table) logDescriptor() string {
	return fmt.Sprintf("(table=[name=%s])", t.name)
}
