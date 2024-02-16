package storage

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

func ReadAll(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

type Partial struct {
	OffsetFrom int64
	OffsetTo   int64
}

func ReadPartials(filename string, partials []*Partial) ([][]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	res := make([][]byte, len(partials))
	for i, partial := range partials {
		res[i] = make([]byte, partial.OffsetTo-partial.OffsetFrom)
		if _, err := file.ReadAt(res[i], partial.OffsetFrom); err != nil {
			_ = file.Close()
			return nil, errors.Wrapf(err, "could not read from file [filename=%s] partial [offsetFrom=%d, offsetTo=%d]", filename, partial.OffsetFrom, partial.OffsetTo)
		}
	}

	return res, file.Close()
}

func ReadAfter(filename string, offset int64) ([]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "could not skip file [filename=%s] data to offset [offset=%d]", filename, offset)
	}

	res, err := io.ReadAll(file)
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "could not read file [filename=%s] data from offset [offset=%d]", filename, offset)
	}

	return res, file.Close()
}

func ReadBefore(filename string, offset int64) ([]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	res := make([]byte, offset)
	if _, err := file.Read(res); err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "could not read file [filename=%s] data to offset [offset=%d]", filename, offset)
	}

	return res, file.Close()
}
