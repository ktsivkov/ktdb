package data

import (
	"reflect"
)

type Column interface {
	Identifier() string
	Type(size int) string
	Marshal(size int) ([]byte, error)
	Unmarshal(size int, payload []byte) (Column, error)
}

type ColumnProcessor interface {
	ReflectionType(identifier string) (reflect.Type, error)
	FromReflectionType(columnType reflect.Type, size int, payload []byte) (Column, error)
}
