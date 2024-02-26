package table

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine/grid/row"
	"ktdb/pkg/storage"
)

func NewProcessor(reader storage.Reader, writer storage.Writer) Processor {
	return &processor{
		reader: reader,
		writer: writer,
	}
}

type Processor interface {
	New(name string, schema *row.Schema) (Table, error)
	Load(name string) (Table, error)
}

type processor struct {
	reader storage.Reader
	writer storage.Writer
}

func (p *processor) New(name string, schema *row.Schema) (Table, error) {
	tbl := &table{reader: p.reader, writer: p.writer, schema: schema, name: name}
	if err := tbl.create(); err != nil {
		return nil, errors.Wrap(err, "could not create table")
	}
	return tbl, nil
}

func (p *processor) Load(name string) (Table, error) {
	tbl := &table{
		reader: p.reader,
		writer: p.writer,
		schema: nil,
		name:   name,
	}
	if err := tbl.load(); err != nil {
		return nil, errors.Wrap(err, "could not load table")
	}
	return tbl, nil
}
