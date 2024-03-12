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

func (t *tokensMock) PopIf(conditions ...tokenizer.Cond) *tokenizer.Token {
	return t.Called(conditions).Get(0).(*tokenizer.Token)
}

func (t *tokensMock) Next() *tokenizer.Token {
	return t.Called().Get(0).(*tokenizer.Token)
}

func (t *tokensMock) NextIf(conditions ...tokenizer.Cond) *tokenizer.Token {
	return t.Called(conditions).Get(0).(*tokenizer.Token)
}
