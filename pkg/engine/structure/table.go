package structure

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"ktdb/pkg/engine/grid/row"
	"ktdb/pkg/engine/storage"
)

const tblDataFile = "data.bin"
const tblSchemaFile = "schema.bin"

type Table interface {
	Name() string
	Schema() *row.Schema
	Row(id int64) (row.Row, error)
	Set(id int64, r row.Row) error
	Append(r row.Row) error
	TotalRows() (int64, error)
	Delete(ctx context.Context) error
}

type table struct {
	storage storage.Storage
	// schema is set by load
	schema *row.Schema
	name   string
}

func (t *table) Name() string {
	return t.name
}

func (t *table) TotalRows() (int64, error) {
	info, err := t.storage.Info(tblDataFile)
	if err != nil {
		return 0, errors.Wrapf(err, "%s could not read data file info", t.errorDescriptor())
	}

	return info.Size() / t.schema.ByteSize(), nil
}

func (t *table) Schema() *row.Schema {
	return t.schema
}

func (t *table) Row(id int64) (row.Row, error) {
	if id < 1 {
		return nil, errors.Errorf("%s invalid row %s", t.errorDescriptor(), t.rowErrorDescriptor(id))
	}

	rowBytes, err := t.storage.ReadPartials(tblDataFile, []*storage.Partial{
		{
			OffsetFrom: t.schema.ByteSize() * (id - 1),
			OffsetTo:   t.schema.ByteSize() * id,
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not read row %s", t.errorDescriptor(), t.rowErrorDescriptor(id))
	}

	return rowBytes[0], nil
}

func (t *table) Set(id int64, r row.Row) error {
	if id < 1 {
		return errors.Errorf("%s invalid row %s", t.errorDescriptor(), t.rowErrorDescriptor(id))
	}
	if err := t.storage.Offset(tblDataFile, t.schema.ByteSize()*(id-1), r); err != nil {
		return errors.Wrapf(err, "%s could not set row %s", t.errorDescriptor(), t.rowErrorDescriptor(id))
	}
	return nil
}

func (t *table) Append(r row.Row) error {
	if err := t.storage.Append(tblDataFile, r); err != nil {
		return errors.Wrapf(err, "%s could not append row", t.errorDescriptor())
	}
	return nil
}

func (t *table) Delete(_ context.Context) error {
	if err := t.storage.Delete(tblDataFile); err != nil {
		return errors.Wrapf(err, "%s could not delete data file", t.errorDescriptor())
	}
	if err := t.storage.Delete(tblSchemaFile); err != nil {
		return errors.Wrapf(err, "%s could not delete schema file", t.errorDescriptor())
	}
	return nil
}

func (t *table) load() error {
	schemaPayload, err := t.storage.ReadAll(tblSchemaFile)
	if err != nil {
		return errors.Wrapf(err, "%s could not read schema", t.errorDescriptor())
	}

	t.schema = &row.Schema{}
	if err := t.schema.Load(schemaPayload); err != nil {
		return errors.Wrapf(err, "%s could not load schema", t.errorDescriptor())
	}

	return nil
}

func (t *table) create() error {
	schemaPayload, err := t.schema.Bytes()
	if err != nil {
		return errors.Wrapf(err, "%s could not get schema bytes", t.errorDescriptor())
	}
	if err := t.storage.CreateOrOverride(tblSchemaFile, schemaPayload); err != nil {
		return errors.Wrapf(err, "%s could not create schema file", t.errorDescriptor())
	}
	if err := t.storage.CreateOrOverride(tblDataFile, nil); err != nil {
		return errors.Wrapf(err, "%s could not create data file", t.errorDescriptor())
	}
	return nil
}

func (t *table) rowErrorDescriptor(id int64) string {
	return fmt.Sprintf("(row=[id=%d])", id)
}

func (t *table) errorDescriptor() string {
	return fmt.Sprintf("(table=[name=%s])", t.name)
}
