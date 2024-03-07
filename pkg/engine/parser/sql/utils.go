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

	for tokens.HasNext() {
		token := tokens.PopIf(tokenizer.TokenComma, tokenizer.TokenKeyword, tokenizer.TokenAsterisk)
		if token == nil {
			if expectsNext {
				return nil, errors.Errorf("invalid column name (%s)", tokens.Next().Value)
			}
			break
		}

		if token.Type == tokenizer.TokenComma {
			if expectsNext {
				return nil, errors.Errorf("invalid column name (%s)", token.Value)
			}
			expectsNext = true
			continue
		}

		if !expectsNext {
			return nil, errors.Errorf("unexpected column name (%s)", token.Value)
		}

		expectsNext = false
		col = &parser.Column{
			Name: token.Value,
		}
		cols = append(cols, col)
	}

	if cols == nil {
		return nil, errors.New("query has no columns specified")
	}

	if expectsNext {
		return nil, errors.Errorf("expected column name after comma")
	}

	return cols, nil
}

func parseTable(tokens tokenizer.Tokens) (*parser.Table, error) {
	token := tokens.PopIf(tokenizer.TokenKeyword, tokenizer.TokenDoubleQuotedString, tokenizer.TokenSingleQuotedString, tokenizer.TokenLiteralString)
	if token == nil {
		if !tokens.HasNext() {
			return nil, errors.New("table name expected")
		}
		return nil, errors.Errorf("invalid table name (%s)", tokens.Next().Value)
	}
	return &parser.Table{Name: token.Value}, nil
}
