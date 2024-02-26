package storage

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

type Writer interface {
	CreateOrOverride(filename string, data []byte) error
	Append(filename string, data []byte) error
	Offset(filename string, offset int64, data []byte) error
	Replace(filename string, partial *Partial, data []byte) error
}

func NewWriter() Writer {
	return &writer{}
}

type writer struct{}

func (w *writer) CreateOrOverride(filename string, data []byte) error {
	file, err := os.Create(filename)
	if err != nil {
		return errors.Wrapf(err, "%s could not open", filename)
	}

	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write to", filename)
	}

	return file.Close()
}

func (w *writer) Append(filename string, data []byte) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "%s could not open", w.logDescriptor(filename))
	}

	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write to", w.logDescriptor(filename))
	}

	return file.Close()
}

func (w *writer) Offset(filename string, offset int64, data []byte) error {
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "%s could not open", w.logDescriptor(filename))
	}

	if _, err = file.WriteAt(data, offset); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not write to at offset [offset=%d]", w.logDescriptor(filename), offset)
	}

	return file.Close()
}

func (w *writer) Replace(filename string, partial *Partial, data []byte) error {
	file, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "could not open file=[filename=%s]", filename)
	}

	var buffer bytes.Buffer
	if _, err := io.CopyN(&buffer, file, partial.OffsetFrom); err != nil && err != io.EOF {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not copy data to offset [fromOffset=%d]", w.logDescriptor(filename), partial.OffsetFrom)
	}

	if _, err := buffer.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "could not write the data into the buffer")
	}

	if _, err := file.Seek(partial.OffsetTo, io.SeekStart); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not skip data until offset [toOffset=%d]", w.logDescriptor(filename), partial.OffsetTo)
	}

	if _, err := io.Copy(&buffer, file); err != nil && err != io.EOF {
		_ = file.Close()
		return errors.Wrapf(err, "%s could not copy data after offset [toOffset=%d]", w.logDescriptor(filename), partial.OffsetTo)
	}

	if err := os.WriteFile(filename, buffer.Bytes(), 0644); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

func (w *writer) logDescriptor(filename string) string {
	return fmt.Sprintf("(file=[filename=%s])", filename)
}
