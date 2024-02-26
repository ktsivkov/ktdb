package column

import (
	"reflect"

	"github.com/pkg/errors"
)

type Processor interface {
	TypeProcessor(typ Type) (TypeProcessor, error)
}

func NewProcessor(types []TypeProcessor) (Processor, error) {
	p := &processor{
		types: make(map[Type]TypeProcessor, len(types)),
	}
	for _, typ := range types {
		if err := p.registerTypeProcessor(typ); err != nil {
			return nil, errors.Wrap(err, "type processor registration failed")
		}
	}
	return p, nil
}

type processor struct {
	types map[Type]TypeProcessor
}

func (p *processor) TypeProcessor(typ Type) (TypeProcessor, error) {
	typeProcessor, found := p.types[typ]
	if !found {
		return nil, errors.Errorf("(type_processor=[type=%s]) not found", typ.String())
	}

	return typeProcessor, nil
}

func (p *processor) registerTypeProcessor(processor TypeProcessor) error {
	if processor == nil {
		return errors.New("(type_processor=[type=nil]) is not a valid type")
	}

	typeIdentifier := processor.Type()
	if typeIdentifier.Empty() {
		return errors.Errorf("(type_processor=[type=%s]) invalid name", reflect.TypeOf(processor).String())
	}

	_, found := p.types[typeIdentifier]
	if found {
		return errors.Errorf("(type_processor=[type=%s, name=%s]) name already used", reflect.TypeOf(processor).String(), typeIdentifier.String())
	}

	p.types[processor.Type()] = processor
	return nil
}
