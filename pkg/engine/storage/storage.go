package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type Storage interface {
	NewLayer(path string) (Storage, error)
	Reader
	Writer
}

type storage struct {
	path string
	reader
	writer
}

func (p *storage) NewLayer(path string) (Storage, error) {
	return New(fmt.Sprintf("%s%c%s", p.path, filepath.Separator, path))
}

func New(path string) (Storage, error) {
	if path == "" {
		return nil, errors.New("cannot create storager for an empty path")
	}

	if err := os.Mkdir(path, 0755); err != nil && os.IsNotExist(err) {
		return nil, errors.Wrapf(err, "could not create storager=[path=%s]", path)
	}

	return &storage{
		path: path,
		reader: reader{
			path: path,
		},
		writer: writer{
			path: path,
		},
	}, nil
}
