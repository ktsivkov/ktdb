package tokenizer

import (
	"slices"
)

type Token struct {
	Type  TokenType
	Value string
}

type Tokens interface {
	HasNext() bool
	Pop() *Token
	PopIf(tokenTypes ...TokenType) *Token
	// Next will return the next element without removing it from the stack
	// if you want to remove it from the stack upon returning, look at the Pop method
	Next() *Token
	// NextIf will return the next element only if it is one of the provided types without removing it from the stack
	// if you want to remove it from the stack upon returning, look at the PopIf method
	NextIf(tokenTypes ...TokenType) *Token
	// AreNextInOrder returns if the next elements in the stack are the same types as provided in order
	AreNextInOrder(tokenTypes ...TokenType) bool
}

func newTokens(elems []*Token) Tokens {
	return &tokens{
		stack: elems,
		len:   len(elems),
	}
}

type tokens struct {
	stack []*Token
	len   int
}

func (t *tokens) HasNext() bool {
	return t.len > 0
}

func (t *tokens) Pop() *Token {
	if !t.HasNext() {
		return nil
	}
	elem := t.stack[0]
	t.stack = t.stack[1:]
	t.len = t.len - 1
	return elem
}

func (t *tokens) PopIf(tokenTypes ...TokenType) *Token {
	if !t.HasNext() {
		return nil
	}
	elem := t.stack[0]
	if !slices.Contains(tokenTypes, elem.Type) {
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

func (t *tokens) NextIf(tokenTypes ...TokenType) *Token {
	if !t.HasNext() {
		return nil
	}
	if elem := t.stack[0]; slices.Contains(tokenTypes, elem.Type) {
		return elem
	}
	return nil
}

func (t *tokens) AreNextInOrder(tokenTypes ...TokenType) bool {
	if len(tokenTypes) > t.len {
		return false
	}

	for i, tokenType := range tokenTypes {
		if t.stack[i].Type != tokenType {
			return false
		}
	}
	return true
}
