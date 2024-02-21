package data

import (
	"reflect"

	"github.com/pkg/errors"
)

func NewTypes(types []reflect.Type) (*Types, error) {
	t := &Types{
		types: make(map[string]reflect.Type, len(types)),
	}
	for _, typ := range types {
		if err := t.register(typ); err != nil {
			return nil, errors.Wrap(err, "type registration failed")
		}
	}
	return t, nil
}

type Types struct {
	types map[string]reflect.Type
}

func (t *Types) Get(identifier string) (reflect.Type, error) {
	typ, found := t.types[identifier]
	if !found {
		return nil, errors.Errorf("(type=[identifier=%s]) not found", identifier)
	}

	return typ, nil
}

func (t *Types) register(p reflect.Type) error {
	if p == nil {
		return errors.Errorf("(type=[type=%s]) is not a valid type", "nil")
	}

	if ct := reflect.TypeOf(new(Column)).Elem(); p.Implements(ct) == false {
		return errors.Errorf("(type=[type=%s]) does not implement [type=%s]", p.String(), ct.String())
	}

	identifier := reflect.New(p).Interface().(Column).Identifier()
	if identifier == "" {
		return errors.Errorf("(type=[type=%s]) invalid identifier", p.String())
	}

	_, found := t.types[identifier]
	if found {
		return errors.Errorf("(type=[type=%s, identifier=%s]) identifier already used", p.String(), identifier)
	}

	t.types[reflect.New(p).Interface().(Column).Identifier()] = p
	return nil
}
