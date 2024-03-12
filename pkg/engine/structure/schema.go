package structure

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"ktdb/pkg/engine/grid/row"
	"ktdb/pkg/engine/storage"
)

type Schema interface {
	Name() string
	List(ctx context.Context) ([]Table, error)
	Get(ctx context.Context, name string) (Table, error)
	Create(ctx context.Context, name string, schema *row.Schema) (Table, error)
	Delete(ctx context.Context) error
}

type schema struct {
	storage storage.Storage
	name    string
}

func (s *schema) Name() string {
	return s.name
}

func (s *schema) List(ctx context.Context) ([]Table, error) {
	tblNames, err := s.storage.List(storage.IsDirFilter)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not list tables", s.errorDescriptor())
	}
	schemas := make([]Table, len(tblNames))
	for i, tblName := range tblNames {
		schemas[i], err = s.Get(ctx, tblName)
		if err != nil {
			return nil, errors.Wrapf(err, "%s could not get table", s.errorDescriptor())
		}
	}
	return schemas, nil
}

func (s *schema) Create(_ context.Context, name string, schema *row.Schema) (Table, error) {
	tableStorage, err := s.storage.NewLayer(name)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not create storage layer", s.errorDescriptor())
	}

	tbl := &table{storage: tableStorage, schema: schema, name: name}
	if err := tbl.create(); err != nil {
		return nil, errors.Wrapf(err, "%s could not create table", s.errorDescriptor())
	}
	return tbl, nil
}

func (s *schema) Get(_ context.Context, name string) (Table, error) {
	tableStorage, err := s.storage.NewLayer(name)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not create storage layer", s.errorDescriptor())
	}

	tbl := &table{
		storage: tableStorage,
		schema:  nil,
		name:    name,
	}
	if err := tbl.load(); err != nil {
		return nil, errors.Wrapf(err, "%s could not load table", s.errorDescriptor())
	}
	return tbl, nil
}

func (s *schema) Delete(ctx context.Context) error {
	tables, err := s.List(ctx)
	if err != nil {
		return errors.Wrapf(err, "%s could not list tables", s.errorDescriptor())
	}
	for _, item := range tables {
		if err := item.Delete(ctx); err != nil {
			return errors.Wrapf(err, "%s could not delete table", s.errorDescriptor())
		}
	}
	return nil
}

func (s *schema) errorDescriptor() string {
	return fmt.Sprintf("(schema=[name=%s])", s.name)
}
