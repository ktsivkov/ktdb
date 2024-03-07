package sql

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine/parser/tokenizer"
)

type WhereOperation int

const (
	WhereUndef WhereOperation = iota
	WhereAnd
	WhereOr
)

const (
	CondEq    = tokenizer.TokenEq
	CondNotEq = tokenizer.TokenNotEq
	CondGt    = tokenizer.TokenGt
	CondLt    = tokenizer.TokenLt
	CondGte   = tokenizer.TokenGte
	CondLte   = tokenizer.TokenLte
)

type WhereCondition struct {
	Target    string
	Operation tokenizer.TokenType
	Value     string
}

type WhereClause struct {
	Left      *WhereClause
	Right     *WhereCondition
	Operation WhereOperation
}

func parseWhereClause(tokens tokenizer.Tokens) (*WhereClause, error) {
	if tokens.PopIf(tokenizer.TokenWhere) == nil {
		return nil, nil
	}

	var (
		clause            *WhereClause
		lastOperatorToken *tokenizer.Token
	)

	for tokens.HasNext() {
		cond, err := parseWhereCondition(tokens)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse WHERE condition")
		}

		clause = &WhereClause{
			Left:      clause,
			Operation: whereOperation(lastOperatorToken),
			Right:     cond,
		}

		lastOperatorToken = tokens.PopIf(tokenizer.TokenAnd, tokenizer.TokenOr)
		if lastOperatorToken == nil {
			break
		}
	}

	if clause == nil {
		return nil, errors.New("no conditions found after WHERE")
	}

	if lastOperatorToken != nil {
		return nil, errors.Errorf("no conditions found after (%s)", lastOperatorToken.Value)
	}

	return clause, nil
}

func parseWhereCondition(tokens tokenizer.Tokens) (*WhereCondition, error) {
	targetToken := tokens.PopIf(tokenizer.TokenKeyword)
	if targetToken == nil {
		if !tokens.HasNext() {
			return nil, errors.New("no target specified")
		}
		return nil, errors.Errorf("invalid target (%s)", tokens.Next().Value)
	}

	operationToken := tokens.PopIf(tokenizer.TokenEq, tokenizer.TokenNotEq, tokenizer.TokenLt, tokenizer.TokenGt, tokenizer.TokenLte, tokenizer.TokenGte)
	if operationToken == nil {
		if !tokens.HasNext() {
			return nil, errors.New("no operation specified")
		}
		return nil, errors.Errorf("invalid operation (%s)", tokens.Next().Value)
	}

	valueToken := tokens.PopIf(tokenizer.TokenLiteralString, tokenizer.TokenSingleQuotedString, tokenizer.TokenDoubleQuotedString, tokenizer.TokenInt, tokenizer.TokenFloat)
	if valueToken == nil {
		if !tokens.HasNext() {
			return nil, errors.New("no value specified")
		}
		return nil, errors.Errorf("invalid value (%s)", tokens.Next().Value)
	}

	return &WhereCondition{
		Target:    targetToken.Value,
		Operation: operationToken.Type,
		Value:     valueToken.Value,
	}, nil
}

func whereOperation(token *tokenizer.Token) WhereOperation {
	if token == nil {
		return WhereUndef
	}
	if token.Type == tokenizer.TokenOr {
		return WhereOr
	}
	return WhereAnd
}
