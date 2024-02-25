package engine

import (
	"fmt"
	"unicode/utf8"

	"github.com/pkg/errors"
)

type ColumnType string

func (t ColumnType) Bytes() []byte {
	return []byte(t)
}

func (t ColumnType) Load(payload []byte) (ColumnType, error) {
	if utf8.Valid(payload) == false {
		return "", errors.Errorf("payload bytes are not valid UTF-8")
	}

	return ColumnType(payload), nil
}

func (t ColumnType) Format(size int) string {
	return fmt.Sprintf("%s[size=%d]", t, size)
}

type Column interface {
	Type() ColumnType
	Bytes(size int) ([]byte, error)
}

type ColumnTypeProcessor interface {
	Type() ColumnType
	Load(size int, payload []byte) (Column, error)
}
