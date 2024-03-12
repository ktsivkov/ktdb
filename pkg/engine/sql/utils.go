package sql

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine/parser"
	"ktdb/pkg/engine/parser/tokenizer"
)

func parseColumns(tokens tokenizer.Tokens) ([]*parser.Column, error) {
	var (
		cols        []*parser.Column
		col         *parser.Column
		expectsNext = true
	)

	for expectsNext && tokens.HasNext() {
		expectsNext = false

		token := tokens.PopIf(tokenizer.IsType(tokenizer.TokenKeyword), tokenizer.IsType(tokenizer.TokenAsterisk))
		if token == nil {
			return nil, errors.Errorf("invalid column name (%s)", tokens.Next().Value)
		}

		col = &parser.Column{
			Name: token.Value,
		}
		cols = append(cols, col)

		if comma := tokens.PopIf(tokenizer.IsType(tokenizer.TokenComma)); comma != nil {
			expectsNext = true
		}
	}

	if cols == nil {
		return nil, errors.New("query has no columns specified")
	}

	return cols, nil
}

func parseTable(tokens tokenizer.Tokens) (*parser.Table, error) {
	token := tokens.PopIf(tokenizer.IsType(tokenizer.TokenKeyword), tokenizer.IsType(tokenizer.TokenDoubleQuotedString), tokenizer.IsType(tokenizer.TokenSingleQuotedString), tokenizer.IsType(tokenizer.TokenLiteralString))
	if token == nil {
		if !tokens.HasNext() {
			return nil, errors.New("table name expected")
		}
		return nil, errors.Errorf("invalid table name (%s)", tokens.Next().Value)
	}
	return &parser.Table{Name: token.Value}, nil
}
