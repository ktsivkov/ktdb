package tokenizer

import (
	"strings"
)

type Token struct {
	Value string
	Type  TokenType
}

func (t *Token) Is(val string, caseSensitive bool) bool {
	if caseSensitive {
		return t.Value == val
	}
	return strings.ToUpper(t.Value) == strings.ToUpper(val)
}

type Tokens interface {
	HasNext() bool
	Pop() *Token
	PopIf(conditions ...Cond) *Token
	// Next will return the next element without removing it from the stack
	// if you want to remove it from the stack upon returning, look at the Pop method
	Next() *Token
	// NextIf will return the next element only if it satisfies one of the conditions
	// if you want to remove it from the stack upon returning, look at the PopIf method
	NextIf(conditions ...Cond) *Token
}

type tokens struct {
	stack []*Token
	len   int
}

func (t *tokens) HasNext() bool {
	return t.len > 0
}

func (t *tokens) Pop() *Token {
	elem := t.Next()
	if elem == nil {
		return nil
	}
	t.stack = t.stack[1:]
	t.len = t.len - 1
	return elem
}

func (t *tokens) PopIf(conditions ...Cond) *Token {
	elem := t.NextIf(conditions...)
	if elem == nil {
		return nil
	}
	t.stack = t.stack[1:]
	t.len = t.len - 1
	return elem
}

func (t *tokens) Next() *Token {
	if !t.HasNext() {
		return nil
	}
	return t.stack[0]
}

func (t *tokens) NextIf(conditions ...Cond) *Token {
	if !t.HasNext() {
		return nil
	}
	elem := t.stack[0]
	for _, cond := range conditions {
		if cond(elem) {
			return elem
		}
	}
	return nil
}
