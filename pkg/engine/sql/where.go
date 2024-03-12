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
	Value     string
	Operation tokenizer.TokenType
}

type WhereClause struct {
	Left      *WhereClause
	Right     *WhereCondition
	Operation WhereOperation
}

func parseWhereClause(tokens tokenizer.Tokens) (*WhereClause, error) {
	if t := tokens.PopIf(tokenizer.CondGroup(tokenizer.IsType(tokenizer.TokenKeyword), tokenizer.Is("WHERE", false))); t == nil {
		return nil, nil
	}

	var (
		clause         *WhereClause
		operationToken *tokenizer.Token
	)

	for tokens.HasNext() {
		cond, err := parseWhereCondition(tokens)
		if err != nil {
			return nil, errors.Wrap(err, "invalid `WHERE` condition")
		}

		clause = &WhereClause{
			Left:      clause,
			Operation: whereOperation(operationToken),
			Right:     cond,
		}
		operationToken = tokens.PopIf(
			tokenizer.CondGroup(tokenizer.IsType(tokenizer.TokenKeyword), tokenizer.Is("AND", false)),
			tokenizer.CondGroup(tokenizer.IsType(tokenizer.TokenKeyword), tokenizer.Is("OR", false)),
		)

		if operationToken == nil {
			break
		}
	}

	if clause == nil {
		return nil, errors.New("no conditions found after `WHERE`")
	}

	if operationToken != nil {
		return nil, errors.Errorf("no conditions found after `%s`", operationToken.Value)
	}

	return clause, nil
}

func parseWhereCondition(tokens tokenizer.Tokens) (*WhereCondition, error) {
	targetToken := tokens.PopIf(tokenizer.IsType(tokenizer.TokenKeyword))
	if targetToken == nil {
		if !tokens.HasNext() {
			return nil, errors.New("no target specified")
		}
		return nil, errors.Errorf("invalid target `%s`", tokens.Next().Value)
	}

	operationToken := tokens.PopIf(
		tokenizer.IsType(tokenizer.TokenEq),
		tokenizer.IsType(tokenizer.TokenNotEq),
		tokenizer.IsType(tokenizer.TokenLt),
		tokenizer.IsType(tokenizer.TokenGt),
		tokenizer.IsType(tokenizer.TokenLte),
		tokenizer.IsType(tokenizer.TokenGte),
	)
	if operationToken == nil {
		if !tokens.HasNext() {
			return nil, errors.New("no operation specified")
		}
		return nil, errors.Errorf("invalid operation `%s`", tokens.Next().Value)
	}

	valueToken := tokens.PopIf(
		tokenizer.IsType(tokenizer.TokenLiteralString),
		tokenizer.IsType(tokenizer.TokenSingleQuotedString),
		tokenizer.IsType(tokenizer.TokenDoubleQuotedString),
		tokenizer.IsType(tokenizer.TokenInt),
		tokenizer.IsType(tokenizer.TokenFloat),
		tokenizer.IsType(tokenizer.TokenGte),
	)
	if valueToken == nil {
		if !tokens.HasNext() {
			return nil, errors.New("no value specified")
		}
		return nil, errors.Errorf("invalid value `%s`", tokens.Next().Value)
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
	if token.Is("AND", false) {
		return WhereAnd
	}
	return WhereOr
}
