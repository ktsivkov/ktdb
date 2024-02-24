package engine

import (
	"reflect"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
)

type ColumnProcessor interface {
	ReflectionType(identifier string) (reflect.Type, error)
	FromReflectionType(columnType reflect.Type, size int, payload []byte) (data.Column, error)
}

func NewColumnProcessor(types []reflect.Type) (ColumnProcessor, error) {
	t := &columnProcessor{
		types: make(map[string]reflect.Type, len(types)),
	}
	for _, typ := range types {
		if err := t.register(typ); err != nil {
			return nil, errors.Wrap(err, "type registration failed")
		}
	}
	return t, nil
}

type columnProcessor struct {
	types map[string]reflect.Type
}

func (p *columnProcessor) FromReflectionType(columnType reflect.Type, size int, payload []byte) (data.Column, error) {
	if columnType.Implements(reflect.TypeOf(new(data.Column)).Elem()) == false {
		return nil, errors.Errorf("invalid column type [%s]", columnType.String())
	}

	res, err := reflect.New(columnType).Interface().(data.Column).Unmarshal(size, payload)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse column")
	}

	return res, nil
}

func (p *columnProcessor) ReflectionType(identifier string) (reflect.Type, error) {
	typ, found := p.types[identifier]
	if !found {
		return nil, errors.Errorf("(type=[identifier=%s]) not found", identifier)
	}

	return typ, nil
}

func (p *columnProcessor) register(typ reflect.Type) error {
	if typ == nil {
		return errors.Errorf("(type=[type=%s]) is not a valid type", "nil")
	}

	if ct := reflect.TypeOf(new(data.Column)).Elem(); typ.Implements(ct) == false {
		return errors.Errorf("(type=[type=%s]) does not implement [type=%s]", typ.String(), ct.String())
	}

	identifier := reflect.New(typ).Interface().(data.Column).Identifier()
	if identifier == "" {
		return errors.Errorf("(type=[type=%s]) invalid identifier", typ.String())
	}

	_, found := p.types[identifier]
	if found {
		return errors.Errorf("(type=[type=%s, identifier=%s]) identifier already used", typ.String(), identifier)
	}

	p.types[reflect.New(typ).Interface().(data.Column).Identifier()] = typ
	return nil
}
