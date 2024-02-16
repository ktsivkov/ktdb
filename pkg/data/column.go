package data

import (
	"reflect"

	"github.com/pkg/errors"
)

type Column interface {
	Unmarshal(size int, payload []byte) (Column, error)
	Marshal(size int) ([]byte, error)
	TypeName(size int) string
}

func ColumnFromType(columnType reflect.Type, size int, payload []byte) (Column, error) {
	if ct := reflect.TypeOf(new(Column)); columnType.Implements(ct.Elem()) == false {
		return nil, errors.Errorf("invalid column type [%s]", columnType.String())
	}

	res, err := reflect.New(columnType).Interface().(Column).Unmarshal(size, payload)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse column")
	}

	return res, nil
}
