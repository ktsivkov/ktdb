package structure

import (
	"context"

	"github.com/pkg/errors"

	"ktdb/pkg/engine/storage"
)

type Structure Processor[Database]

func New(storage storage.Storage) (Structure, error) {
	return &structure{
		storage: storage,
	}, nil
}

type structure struct {
	storage storage.Storage
}

func (s *structure) List(ctx context.Context) ([]Database, error) {
	databaseNames, err := s.storage.List(storage.IsDirFilter)
	if err != nil {
		return nil, errors.Wrap(err, "could not list databases")
	}
	dbs := make([]Database, len(databaseNames))
	for i, dbName := range databaseNames {
		dbs[i], err = s.Get(ctx, dbName)
		if err != nil {
			return nil, errors.Wrap(err, "could not get database")
		}
	}
	return dbs, nil
}

func (s *structure) Create(ctx context.Context, name string) (Database, error) {
	return s.load(ctx, name)
}

func (s *structure) Get(ctx context.Context, name string) (Database, error) {
	return s.load(ctx, name)
}

func (s *structure) Delete(ctx context.Context) error {
	dbs, err := s.List(ctx)
	if err != nil {
		return errors.Wrap(err, "could not list databases")
	}
	for _, item := range dbs {
		if err := item.Delete(ctx); err != nil {
			return errors.Wrap(err, "could not delete database")
		}
	}
	return nil
}

func (s *structure) load(_ context.Context, name string) (Database, error) {
	schemaStorage, err := s.storage.NewLayer(name)
	if err != nil {
		return nil, errors.Wrap(err, "could not create storage layer")
	}

	return &database{name: name, storage: schemaStorage}, nil
}
