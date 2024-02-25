package engine

import (
	"reflect"

	"github.com/pkg/errors"
)

type ColumnProcessor interface {
	FromType(typ ColumnType, size int, payload []byte) (Column, error)
}

func NewColumnProcessor(types []ColumnTypeProcessor) (ColumnProcessor, error) {
	p := &columnProcessor{
		types: make(map[ColumnType]ColumnTypeProcessor, len(types)),
	}
	for _, typ := range types {
		if err := p.register(typ); err != nil {
			return nil, errors.Wrap(err, "type registration failed")
		}
	}
	return p, nil
}

type columnProcessor struct {
	types map[ColumnType]ColumnTypeProcessor
}

func (p *columnProcessor) FromType(typ ColumnType, size int, payload []byte) (Column, error) {
	processor, err := p.getProcessor(typ)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load processor")
	}

	res, err := processor.Load(size, payload)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse column")
	}

	return res, nil
}

func (p *columnProcessor) getProcessor(typeIdentifier ColumnType) (ColumnTypeProcessor, error) {
	processor, found := p.types[typeIdentifier]
	if !found {
		return nil, errors.Errorf("(processor=[identifier=%s]) not found", typeIdentifier)
	}

	return processor, nil
}

func (p *columnProcessor) register(processor ColumnTypeProcessor) error {
	if processor == nil {
		return errors.Errorf("(type=[type=%s]) is not a valid type", "nil")
	}

	typeIdentifier := processor.Type()
	if typeIdentifier == "" {
		return errors.Errorf("(type=[type=%s]) invalid identifier", reflect.TypeOf(processor).String())
	}

	_, found := p.types[typeIdentifier]
	if found {
		return errors.Errorf("(type=[type=%s, identifier=%s]) identifier already used", reflect.TypeOf(processor).String(), typeIdentifier)
	}

	p.types[processor.Type()] = processor
	return nil
}
