package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type Reader interface {
	Info(filename string) (os.FileInfo, error)
	ReadAll(filename string) ([]byte, error)
	ReadPartials(filename string, partials []*Partial) ([][]byte, error)
	ReadAfter(filename string, offset int64) ([]byte, error)
	ReadBefore(filename string, offset int64) ([]byte, error)
	List(filters ...FileFilter) ([]string, error)
}

type reader struct {
	path string
}

func (r *reader) Info(filename string) (os.FileInfo, error) {
	return os.Stat(filename)
}

func (r *reader) ReadAll(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func (r *reader) ReadPartials(filename string, partials []*Partial) ([][]byte, error) {
	file, err := os.OpenFile(r.pathToFile(filename), os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not open file", r.errorDescriptor(filename))
	}

	res := make([][]byte, len(partials))
	for i, partial := range partials {
		res[i] = make([]byte, partial.OffsetTo-partial.OffsetFrom)
		if _, err := file.ReadAt(res[i], partial.OffsetFrom); err != nil {
			_ = file.Close()
			return nil, errors.Wrapf(err, "%s could not read from file partial [offsetFrom=%d, offsetTo=%d]", r.errorDescriptor(filename), partial.OffsetFrom, partial.OffsetTo)
		}
	}

	return res, file.Close()
}

func (r *reader) ReadAfter(filename string, offset int64) ([]byte, error) {
	file, err := os.OpenFile(r.pathToFile(filename), os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not open file", r.errorDescriptor(filename))
	}

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "%s could not skip file data to offset [offset=%d]", r.errorDescriptor(filename), offset)
	}

	res, err := io.ReadAll(file)
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "%s could not read file data from offset [offset=%d]", r.errorDescriptor(filename), offset)
	}

	return res, file.Close()
}

func (r *reader) ReadBefore(filename string, offset int64) ([]byte, error) {
	file, err := os.OpenFile(r.pathToFile(filename), os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not open file", r.errorDescriptor(filename))
	}

	res := make([]byte, offset)
	if _, err := file.Read(res); err != nil {
		_ = file.Close()
		return nil, errors.Wrapf(err, "%s could not read data to offset [offset=%d]", r.errorDescriptor(filename), offset)
	}

	return res, file.Close()
}

func (r *reader) List(filters ...FileFilter) ([]string, error) {
	entries, err := os.ReadDir(r.path)
	if err != nil {
		return nil, errors.Wrapf(err, "%s could not read path", r.errorDescriptor(""))
	}

	matches := make([]string, 0, len(entries))
EntryLoop:
	for _, entry := range entries {
		for _, filter := range filters {
			filterRes, err := filter(entry)
			if err != nil {
				return nil, errors.Wrapf(err, "%s filter failed on entry=[name=%s]", r.errorDescriptor(""), entry.Name())
			}
			if !filterRes {
				continue EntryLoop
			}
		}
		matches = append(matches, entry.Name())
	}

	return matches, nil
}

func (r *reader) pathToFile(filename string) string {
	return fmt.Sprintf("%s%c%s", r.path, filepath.Separator, filename)
}

func (r *reader) errorDescriptor(filename string) string {
	if filename == "" {
		return fmt.Sprintf("(file=[path=%s])", r.path)
	}
	return fmt.Sprintf("(file=[filename=%s, path=%s])", filename, r.path)
}
