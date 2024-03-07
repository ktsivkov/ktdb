package parser

import (
	"ktdb/pkg/engine/parser/tokenizer"
)

type Statement interface {
	Json() (string, error)
}

type StatementParser interface {
	Is(tokens tokenizer.Tokens) bool
	Parse(tokens tokenizer.Tokens) (Statement, error)
}

type Column struct {
	Name string
}

type Table struct {
	Name string
}
