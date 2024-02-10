package table

import (
	"reflect"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
)

type ColumnSchema struct {
	Name    string
	Default data.Column
	Type    reflect.Type
	Rules   []ColumnRuleFunc
}

func (s *ColumnSchema) Validate(col data.Column) error {
	rules := append(s.Rules, WithTypeOf(s.Type))
	for _, rule := range rules {
		if err := rule(col); err != nil {
			return errors.Wrap(err, "validation failed")
		}
	}

	return nil
}

type ColumnRuleFunc func(col data.Column) error

func WithTypeOf(typ reflect.Type) ColumnRuleFunc {
	return func(col data.Column) error {
		if col == nil {
			return nil // It is okay since it can
		}

		if reflect.TypeOf(col) != typ {
			wantedTypeName := reflect.New(typ).Interface().(data.Column).TypeName()
			return errors.Errorf("given type [%s] doesn't match required type [%s]", col.TypeName(), wantedTypeName)
		}
		return nil
	}
}

func WithNotNullValueRule() ColumnRuleFunc {
	return func(col data.Column) error {
		if col == nil {
			return errors.New("cannot be null")
		}
		return nil
	}
}
