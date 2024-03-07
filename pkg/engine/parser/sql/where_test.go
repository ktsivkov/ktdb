package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/engine/parser/tokenizer"
)

func TestParseWhereClause(t *testing.T) {

}

func TestParseWhereCondition(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expected := &WhereCondition{
			Target:    "test-target",
			Operation: CondEq,
			Value:     "test-value",
		}
		tokens := &tokensMock{}
		tokens.On("PopIf", []tokenizer.TokenType{tokenizer.TokenKeyword}).Return(&tokenizer.Token{
			Value: expected.Target,
		})
		tokens.On("PopIf", []tokenizer.TokenType{
			tokenizer.TokenEq, tokenizer.TokenNotEq, tokenizer.TokenLt, tokenizer.TokenGt, tokenizer.TokenLte, tokenizer.TokenGte,
		}).Return(&tokenizer.Token{
			Type: expected.Operation,
		})
		tokens.On("PopIf", []tokenizer.TokenType{
			tokenizer.TokenLiteralString, tokenizer.TokenSingleQuotedString, tokenizer.TokenDoubleQuotedString, tokenizer.TokenInt, tokenizer.TokenFloat,
		}).Return(&tokenizer.Token{
			Value: expected.Value,
		})
		res, err := parseWhereCondition(tokens)
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("target", func(t *testing.T) {
			t.Run("not set", func(t *testing.T) {
				tokens := &tokensMock{}
				var ret *tokenizer.Token
				tokens.On("PopIf", []tokenizer.TokenType{tokenizer.TokenKeyword}).Return(ret)
				tokens.On("HasNext").Return(false)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "no target specified")
			})
			t.Run("not matching", func(t *testing.T) {
				tokens := &tokensMock{}
				var ret *tokenizer.Token
				tokens.On("PopIf", []tokenizer.TokenType{tokenizer.TokenKeyword}).Return(ret)
				tokens.On("Next").Return(&tokenizer.Token{
					Type:  0,
					Value: "invalid token",
				})
				tokens.On("HasNext").Return(true)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "invalid target (invalid token)")
			})
		})
		t.Run("operation", func(t *testing.T) {
			t.Run("not set", func(t *testing.T) {
				tokens := &tokensMock{}
				tokens.On("PopIf", []tokenizer.TokenType{tokenizer.TokenKeyword}).Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", []tokenizer.TokenType{
					tokenizer.TokenEq, tokenizer.TokenNotEq, tokenizer.TokenLt, tokenizer.TokenGt, tokenizer.TokenLte, tokenizer.TokenGte,
				}).Return(ret)
				tokens.On("HasNext").Return(false)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "no operation specified")
			})
			t.Run("not matching", func(t *testing.T) {
				tokens := &tokensMock{}
				tokens.On("PopIf", []tokenizer.TokenType{tokenizer.TokenKeyword}).Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", []tokenizer.TokenType{
					tokenizer.TokenEq, tokenizer.TokenNotEq, tokenizer.TokenLt, tokenizer.TokenGt, tokenizer.TokenLte, tokenizer.TokenGte,
				}).Return(ret)
				tokens.On("Next").Return(&tokenizer.Token{
					Type:  0,
					Value: "invalid token",
				})
				tokens.On("HasNext").Return(true)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "invalid operation (invalid token)")
			})
		})
		t.Run("value", func(t *testing.T) {
			t.Run("not set", func(t *testing.T) {
				tokens := &tokensMock{}
				tokens.On("PopIf", []tokenizer.TokenType{tokenizer.TokenKeyword}).Return(&tokenizer.Token{})
				tokens.On("PopIf", []tokenizer.TokenType{
					tokenizer.TokenEq, tokenizer.TokenNotEq, tokenizer.TokenLt, tokenizer.TokenGt, tokenizer.TokenLte, tokenizer.TokenGte,
				}).Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", []tokenizer.TokenType{
					tokenizer.TokenLiteralString, tokenizer.TokenSingleQuotedString, tokenizer.TokenDoubleQuotedString, tokenizer.TokenInt, tokenizer.TokenFloat,
				}).Return(ret)
				tokens.On("HasNext").Return(false)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "no value specified")
			})
			t.Run("not matching", func(t *testing.T) {
				tokens := &tokensMock{}
				tokens.On("PopIf", []tokenizer.TokenType{tokenizer.TokenKeyword}).Return(&tokenizer.Token{})
				tokens.On("PopIf", []tokenizer.TokenType{
					tokenizer.TokenEq, tokenizer.TokenNotEq, tokenizer.TokenLt, tokenizer.TokenGt, tokenizer.TokenLte, tokenizer.TokenGte,
				}).Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", []tokenizer.TokenType{
					tokenizer.TokenLiteralString, tokenizer.TokenSingleQuotedString, tokenizer.TokenDoubleQuotedString, tokenizer.TokenInt, tokenizer.TokenFloat,
				}).Return(ret)
				tokens.On("Next").Return(&tokenizer.Token{
					Type:  0,
					Value: "invalid token",
				})
				tokens.On("HasNext").Return(true)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "invalid value (invalid token)")
			})
		})
	})
}

func TestWhereOperation(t *testing.T) {
	type testCase struct {
		given    *tokenizer.Token
		expected WhereOperation
	}
	tests := map[string]testCase{
		"NONE": {
			given:    nil,
			expected: WhereUndef,
		},
		"AND": {
			given: &tokenizer.Token{
				Type:  tokenizer.TokenAnd,
				Value: "AND",
			},
			expected: WhereAnd,
		},
		"OR": {
			given: &tokenizer.Token{
				Type:  tokenizer.TokenOr,
				Value: "OR",
			},
			expected: WhereOr,
		},
	}
	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, test.expected, whereOperation(test.given))
		})
	}
}
