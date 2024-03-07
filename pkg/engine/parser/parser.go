package parser

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine/parser/tokenizer"
)

func NewSqlParser(sqlTokenizer tokenizer.SqlTokenizer, stmtParsers []StatementParser) (Parser, error) {
	if sqlTokenizer == nil {
		return nil, errors.New("undefined sql tokenizer")
	}
	for _, stmtParser := range stmtParsers {
		if stmtParser == nil {
			return nil, errors.New("undefined statement parser")
		}
	}
	return &sqlParser{stmtParsers: stmtParsers, sqlTokenizer: sqlTokenizer}, nil
}

type Parser interface {
	Parse(query string) (Statement, error)
}

type sqlParser struct {
	stmtParsers  []StatementParser
	sqlTokenizer tokenizer.SqlTokenizer
}

func (p *sqlParser) Parse(query string) (Statement, error) {
	tokens := p.sqlTokenizer.Parse(query)
	for _, stmtParser := range p.stmtParsers {
		if stmtParser.Is(tokens) {
			stmt, err := stmtParser.Parse(tokens)
			if err != nil {
				return nil, errors.Wrap(err, "invalid query")
			}
			return stmt, nil
		}
	}
	return nil, errors.New("unknown query type")
}
