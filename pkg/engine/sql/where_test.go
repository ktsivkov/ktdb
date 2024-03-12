package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"ktdb/pkg/engine/parser/tokenizer"
)

func TestParseWhereClause(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("with clause", func(t *testing.T) {
			expectedCond := &WhereCondition{
				Target:    "test-target",
				Operation: CondEq,
				Value:     "test-value",
			}
			expected := &WhereClause{
				Left:      nil,
				Right:     expectedCond,
				Operation: WhereUndef,
			}

			tokens := &tokensMock{}
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
			tokens.On("HasNext").Once().Return(true)
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
				Value: expectedCond.Target,
			})
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
				Type: expectedCond.Operation,
			})
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
				Value: expectedCond.Value,
			})
			var ret *tokenizer.Token
			tokens.On("PopIf", mock.Anything).Once().Return(ret)

			res, err := parseWhereClause(tokens)
			assert.NoError(t, err)
			assert.Equal(t, expected, res)
		})
		t.Run("with no clause", func(t *testing.T) {
			tokens := &tokensMock{}
			var ret *tokenizer.Token
			tokens.On("PopIf", mock.Anything).Once().Return(ret)
			res, err := parseWhereClause(tokens)
			assert.NoError(t, err)
			assert.Nil(t, res)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("no clause after where", func(t *testing.T) {
			t.Run("with clause", func(t *testing.T) {
				tokens := &tokensMock{}
				tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
				tokens.On("HasNext").Once().Return(true)
				tokens.On("HasNext").Once().Return(false)
				var ret *tokenizer.Token
				tokens.On("PopIf", mock.Anything).Once().Return(ret)

				res, err := parseWhereClause(tokens)
				assert.EqualError(t, err, "invalid `WHERE` condition: no target specified")
				assert.Nil(t, res)
			})
		})
		t.Run("no clauses defined", func(t *testing.T) {
			tokens := &tokensMock{}
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
			tokens.On("HasNext").Once().Return(false)
			var ret *tokenizer.Token
			tokens.On("PopIf", mock.Anything).Once().Return(ret)

			res, err := parseWhereClause(tokens)
			assert.EqualError(t, err, "no conditions found after `WHERE`")
			assert.Nil(t, res)
		})
		t.Run("no clauses after operation", func(t *testing.T) {
			expectedCond := &WhereCondition{
				Target:    "test-target",
				Operation: CondEq,
				Value:     "test-value",
			}

			tokens := &tokensMock{}
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
			tokens.On("HasNext").Once().Return(true)
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
				Value: expectedCond.Target,
			})
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
				Type: expectedCond.Operation,
			})
			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
				Value: expectedCond.Value,
			})

			tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
				Value: "AND",
			})
			tokens.On("HasNext").Once().Return(false)

			res, err := parseWhereClause(tokens)
			assert.EqualError(t, err, "no conditions found after `AND`")
			assert.Nil(t, res)
		})
	})
}

func TestParseWhereCondition(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expected := &WhereCondition{
			Target:    "test-target",
			Operation: CondEq,
			Value:     "test-value",
		}
		tokens := &tokensMock{}
		tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
			Value: expected.Target,
		})
		tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
			Type: expected.Operation,
		})
		tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{
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
				tokens.On("PopIf", mock.Anything).Once().Return(ret)
				tokens.On("HasNext").Return(false)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "no target specified")
			})
			t.Run("not matching", func(t *testing.T) {
				tokens := &tokensMock{}
				var ret *tokenizer.Token
				tokens.On("PopIf", mock.Anything).Once().Return(ret)
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
				tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", mock.Anything).Once().Return(ret)
				tokens.On("HasNext").Return(false)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "no operation specified")
			})
			t.Run("not matching", func(t *testing.T) {
				tokens := &tokensMock{}
				tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", mock.Anything).Once().Return(ret)
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
				tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
				tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", mock.Anything).Once().Return(ret)
				tokens.On("HasNext").Return(false)
				res, err := parseWhereCondition(tokens)
				assert.Nil(t, res)
				assert.EqualError(t, err, "no value specified")
			})
			t.Run("not matching", func(t *testing.T) {
				tokens := &tokensMock{}
				tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
				tokens.On("PopIf", mock.Anything).Once().Return(&tokenizer.Token{})
				var ret *tokenizer.Token
				tokens.On("PopIf", mock.Anything).Once().Return(ret)
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
				Type:  tokenizer.TokenKeyword,
				Value: "AND",
			},
			expected: WhereAnd,
		},
		"OR": {
			given: &tokenizer.Token{
				Type:  tokenizer.TokenKeyword,
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
