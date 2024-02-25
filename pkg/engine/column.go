package engine

import "fmt"

type ColumnType string

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
