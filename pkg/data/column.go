package data

import (
	"reflect"

	"github.com/pkg/errors"

	"ktdb/pkg/payload"
)

type Column interface {
	Unmarshal(payload payload.Payload) (Column, error)
	Marshal() (payload.Payload, error)
	TypeName() string
}

func ColumnFromType(columnType reflect.Type, payload payload.Payload) (Column, error) {
	if ct := reflect.TypeOf(new(Column)); columnType.Implements(ct.Elem()) == false {
		return nil, errors.Errorf("invalid column type [%s]", columnType.String())
	}

	res, err := reflect.New(columnType).Interface().(Column).Unmarshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse column")
	}

	return res, nil
}
