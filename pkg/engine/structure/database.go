package structure

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"ktdb/pkg/engine/storage"
)

type Database interface {
	Name() string
	Processor[Schema]
}

type database struct {
	storage storage.Storage
	name    string
}

func (d *database) Name() string {
	return d.name
}

func (d *database) List(ctx context.Context) ([]Schema, error) {
	schemaNames, err := d.storage.List(storage.IsDirFilter)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not list schemas", d.errorDescriptor())
	}
	schemas := make([]Schema, len(schemaNames))
	for i, schemaName := range schemaNames {
		schemas[i], err = d.Get(ctx, schemaName)
		if err != nil {
			return nil, errors.Wrapf(err, "%s could not get schema", d.errorDescriptor())
		}
	}
	return schemas, nil
}

func (d *database) Create(ctx context.Context, name string) (Schema, error) {
	return d.load(ctx, name)
}

func (d *database) Get(ctx context.Context, name string) (Schema, error) {
	return d.load(ctx, name)
}

func (d *database) Delete(ctx context.Context) error {
	schemas, err := d.List(ctx)
	if err != nil {
		return errors.Wrapf(err, "%s could not list schemas", d.errorDescriptor())
	}
	for _, item := range schemas {
		if err := item.Delete(ctx); err != nil {
			return errors.Wrapf(err, "%s could not delete schema", d.errorDescriptor())
		}
	}
	return nil
}

func (d *database) load(_ context.Context, name string) (Schema, error) {
	schemaStorage, err := d.storage.NewLayer(name)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not create storage layer", d.errorDescriptor())
	}

	return &schema{name: name, storage: schemaStorage}, nil
}

func (d *database) errorDescriptor() string {
	return fmt.Sprintf("(database=[name=%s])", d.name)
}
