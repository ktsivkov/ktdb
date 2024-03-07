package sql

import (
	"encoding/json"

	"github.com/pkg/errors"

	"ktdb/pkg/engine/parser"
	"ktdb/pkg/engine/parser/tokenizer"
)

func NewSelectParser() parser.StatementParser {
	return &selectParser{}
}

type selectStatement struct {
	Columns []*parser.Column
	Table   *parser.Table
	Where   *WhereClause
}

func (s *selectStatement) Json() (string, error) {
	res, err := json.Marshal(s)
	if err != nil {
		return "", errors.Wrap(err, "could not generate json for statement")
	}
	return string(res), nil
}

type selectParser struct {
}

func (s *selectParser) Is(tokens tokenizer.Tokens) bool {
	return tokens.PopIf(tokenizer.TokenSelect) != nil
}

func (s *selectParser) Parse(tokens tokenizer.Tokens) (parser.Statement, error) {
	var (
		stmt = &selectStatement{}
		err  error
	)
	stmt.Columns, err = parseColumns(tokens) // Ignore the first token -- query identifier
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query columns")
	}

	if tokens.HasNext() {
		stmt.Table, err = s.parseFrom(tokens)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse query table")
		}

		stmt.Where, err = parseWhereClause(tokens)
		if err != nil {
			if err != nil {
				return nil, errors.Wrap(err, "could not parse WHERE clause")
			}
		}

		if tokens.HasNext() {
			return nil, errors.Errorf("unexpected symbol (%s)", tokens.Next().Value)
		}
	}

	return stmt, nil
}

func (s *selectParser) parseFrom(tokens tokenizer.Tokens) (*parser.Table, error) {
	token := tokens.PopIf(tokenizer.TokenFrom)
	if token == nil {
		return nil, errors.Errorf("expected FROM got (%s)", tokens.Next().Value)
	}

	return parseTable(tokens)
}
