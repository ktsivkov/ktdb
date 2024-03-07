package sql

import (
	"github.com/stretchr/testify/mock"

	"ktdb/pkg/engine/parser/tokenizer"
)

type tokensMock struct {
	mock.Mock
}

func (t *tokensMock) HasNext() bool {
	return t.Called().Bool(0)
}

func (t *tokensMock) Pop() *tokenizer.Token {
	return t.Called().Get(0).(*tokenizer.Token)
}

func (t *tokensMock) PopIf(tokenTypes ...tokenizer.TokenType) *tokenizer.Token {
	return t.Called(tokenTypes).Get(0).(*tokenizer.Token)
}

func (t *tokensMock) Next() *tokenizer.Token {
	return t.Called().Get(0).(*tokenizer.Token)
}

func (t *tokensMock) NextIf(tokenTypes ...tokenizer.TokenType) *tokenizer.Token {
	return t.Called(tokenTypes).Get(0).(*tokenizer.Token)
}

func (t *tokensMock) AreNextInOrder(tokenTypes ...tokenizer.TokenType) bool {
	return t.Called(tokenTypes).Bool(0)
}
