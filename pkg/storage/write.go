package storage

import (
	"bytes"
	"io"
	"os"

	"github.com/pkg/errors"
)

func CreateOrOverride(filename string, data []byte) error {
	file, err := os.Create(filename)
	if err != nil {
		return errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "could not write to file [filename=%s]", filename)
	}

	return file.Close()
}

func Append(filename string, data []byte) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "could not write to file [filename=%s]", filename)
	}

	return file.Close()
}

func Offset(filename string, offset int64, data []byte) error {
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	if _, err = file.WriteAt(data, offset); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "could not write to file [filename=%s] at offset [offset=%d]", filename, offset)
	}

	return file.Close()
}

func Replace(filename string, fromOffset int64, toOffset int64, data []byte) error {
	file, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "could not open file [filename=%s]", filename)
	}

	var buffer bytes.Buffer
	if _, err := io.CopyN(&buffer, file, fromOffset); err != nil && err != io.EOF {
		_ = file.Close()
		return errors.Wrapf(err, "could not copy file [filename=%s] data to offset [fromOffset=%d]", filename, fromOffset)
	}

	if _, err := buffer.Write(data); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "could not write the data into the buffer")
	}

	if _, err := file.Seek(toOffset, io.SeekStart); err != nil {
		_ = file.Close()
		return errors.Wrapf(err, "could not skip file [filename=%s] data until offset [toOffset=%d]", filename, toOffset)
	}

	if _, err := io.Copy(&buffer, file); err != nil && err != io.EOF {
		_ = file.Close()
		return errors.Wrapf(err, "could not copy file [filename=%s] data after offset [toOffset=%d]", filename, toOffset)
	}

	if err := os.WriteFile(filename, buffer.Bytes(), 0644); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}
