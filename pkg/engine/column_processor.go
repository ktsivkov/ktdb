package engine

import (
	"reflect"

	"github.com/pkg/errors"
)

type ColumnProcessor interface {
	FromType(typeIdentifier string, size int, payload []byte) (Column, error)
}

func NewColumnProcessor(types []reflect.Type) (ColumnProcessor, error) {
	p := &columnProcessor{
		types: make(map[string]reflect.Type, len(types)),
	}
	for _, typ := range types {
		if err := p.register(typ); err != nil {
			return nil, errors.Wrap(err, "type registration failed")
		}
	}
	return p, nil
}

type columnProcessor struct {
	types map[string]reflect.Type
}

func (p *columnProcessor) FromType(typeIdentifier string, size int, payload []byte) (Column, error) {
	columnType, err := p.get(typeIdentifier)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get reflection type")
	}

	res, err := reflect.New(columnType).Interface().(Column).Unmarshal(size, payload)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse column")
	}

	return res, nil
}

func (p *columnProcessor) get(typeIdentifier string) (reflect.Type, error) {
	typ, found := p.types[typeIdentifier]
	if !found {
		return nil, errors.Errorf("(type=[identifier=%s]) not found", typeIdentifier)
	}

	return typ, nil
}

func (p *columnProcessor) register(typ reflect.Type) error {
	if typ == nil {
		return errors.Errorf("(type=[type=%s]) is not a valid type", "nil")
	}

	if ct := reflect.TypeOf(new(Column)).Elem(); typ.Implements(ct) == false {
		return errors.Errorf("(type=[type=%s]) does not implement [type=%s]", typ.String(), ct.String())
	}

	identifier := reflect.New(typ).Interface().(Column).TypeIdentifier()
	if identifier == "" {
		return errors.Errorf("(type=[type=%s]) invalid identifier", typ.String())
	}

	_, found := p.types[identifier]
	if found {
		return errors.Errorf("(type=[type=%s, identifier=%s]) identifier already used", typ.String(), identifier)
	}

	p.types[reflect.New(typ).Interface().(Column).TypeIdentifier()] = typ
	return nil
}
