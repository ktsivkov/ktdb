package storage

import (
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

type Reader interface {
	ReadAll(filename string) ([]byte, error)
	ReadPartials(filename string, partials []*Partial) ([][]byte, error)
	ReadAfter(filename string, offset int64) ([]byte, error)
	ReadBefore(filename string, offset int64) ([]byte, error)
}

func NewReader() Reader {
	return &reader{}
}

type reader struct{}

func (r *reader) ReadAll(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func (r *reader) ReadPartials(filename string, partials []*Partial) ([][]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not open file", r.logDescriptor(filename))
	}

	res := make([][]byte, len(partials))
	for i, partial := range partials {
		res[i] = make([]byte, partial.OffsetTo-partial.OffsetFrom)
		if _, err := file.ReadAt(res[i], partial.OffsetFrom); err != nil {
			_ = file.Close()
			return nil, errors.Wrapf(err, "%s could not read from file partial [offsetFrom=%d, offsetTo=%d]", r.logDescriptor(filename), partial.OffsetFrom, partial.OffsetTo)
		}
	}

	return res, file.Close()
}

func (r *reader) ReadAfter(filename string, offset int64) ([]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "%s could not skip file data to offset [offset=%d]", r.logDescriptor(filename), offset)
	}

	res, err := io.ReadAll(file)
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "%s could not read file data from offset [offset=%d]", r.logDescriptor(filename), offset)
	}

	return res, file.Close()
}

func (r *reader) ReadBefore(filename string, offset int64) ([]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not open file", r.logDescriptor(filename))
	}

	res := make([]byte, offset)
	if _, err := file.Read(res); err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "%s could not read data to offset [offset=%d]", r.logDescriptor(filename), offset)
	}

	return res, file.Close()
}

func (r *reader) logDescriptor(filename string) string {
	return fmt.Sprintf("(file=[filename=%s])", filename)
}
