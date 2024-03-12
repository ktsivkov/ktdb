package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type Writer interface {
	CreateOrOverride(filename string, data []byte) error
	Append(filename string, data []byte) error
	Offset(filename string, offset int64, data []byte) error
	Replace(filename string, partial *Partial, data []byte) error
	Delete(filename string) error
}

type writer struct {
	path string
}

func (w *writer) CreateOrOverride(filename string, data []byte) error {
	file, err := os.Create(w.pathToFile(filename))
	if err != nil {
		return errors.Wrapf(err, "%s could not open file", w.errorDescriptor(filename))
	}

	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write to file", w.errorDescriptor(filename))
	}

	return file.Close()
}

func (w *writer) Append(filename string, data []byte) error {
	file, err := os.OpenFile(w.pathToFile(filename), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "%s could not open file", w.errorDescriptor(filename))
	}

	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write to file", w.errorDescriptor(filename))
	}

	return file.Close()
}

func (w *writer) Offset(filename string, offset int64, data []byte) error {
	file, err := os.OpenFile(w.pathToFile(filename), os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "%s could not open file", w.errorDescriptor(filename))
	}

	if _, err = file.WriteAt(data, offset); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write to file at offset [offset=%d]", w.errorDescriptor(filename), offset)
	}

	return file.Close()
}

func (w *writer) Replace(filename string, partial *Partial, data []byte) error {
	file, err := os.Open(w.pathToFile(filename))
	if err != nil {
		return errors.Wrapf(err, "%s could not open file", w.errorDescriptor(filename))
	}

	var buffer bytes.Buffer
	if _, err := io.CopyN(&buffer, file, partial.OffsetFrom); err != nil && err != io.EOF {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not copy data to offset [fromOffset=%d]", w.errorDescriptor(filename), partial.OffsetFrom)
	}

	if _, err := buffer.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write the data into the buffer", w.errorDescriptor(filename))
	}

	if _, err := file.Seek(partial.OffsetTo, io.SeekStart); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not skip data until offset [toOffset=%d]", w.errorDescriptor(filename), partial.OffsetTo)
	}

	if _, err := io.Copy(&buffer, file); err != nil && err != io.EOF {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not copy data after offset [toOffset=%d]", w.errorDescriptor(filename), partial.OffsetTo)
	}

	if err := os.WriteFile(w.pathToFile(filename), buffer.Bytes(), 0644); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write to file", w.errorDescriptor(filename))
	}

	return file.Close()
}

func (w *writer) Delete(filename string) error {
	if err := os.Remove(w.pathToFile(filename)); err != nil {
		return errors.Wrapf(err, "%s could not delete file", w.errorDescriptor(filename))
	}
	return nil
}

func (w *writer) pathToFile(filename string) string {
	return fmt.Sprintf("%s%c%s", w.path, filepath.Separator, filename)
}

func (w *writer) errorDescriptor(filename string) string {
	return fmt.Sprintf("(file=[filename=%s, path=%s])", filename, w.path)
}
