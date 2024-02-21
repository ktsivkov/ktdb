package data

import (
	"reflect"

	"github.com/pkg/errors"
)

type Column interface {
	Identifier() string
	Type(size int) string
	Marshal(size int) ([]byte, error)
	Unmarshal(size int, payload []byte) (Column, error)
}

func ColumnFromType(columnType reflect.Type, size int, payload []byte) (Column, error) {
	if columnType.Implements(reflect.TypeOf(new(Column)).Elem()) == false {
		return nil, errors.Errorf("invalid column type [%s]", columnType.String())
	}

	res, err := reflect.New(columnType).Interface().(Column).Unmarshal(size, payload)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse column")
	}

	return res, nil
}
