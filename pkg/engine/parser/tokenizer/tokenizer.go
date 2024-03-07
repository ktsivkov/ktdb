package tokenizer

import (
	"github.com/bzick/tokenizer"
)

type TokenType = tokenizer.TokenKey

const ( // Allies for default Tokenizer tokens
	TokenUnknown            TokenType = tokenizer.TokenUnknown        //unspecified token key.
	TokenKeyword            TokenType = tokenizer.TokenKeyword        //keyword, any combination of letters, including unicode letters.
	TokenInt                TokenType = tokenizer.TokenInteger        //integer value
	TokenFloat              TokenType = tokenizer.TokenFloat          //float/double value
	TokenDoubleQuotedString TokenType = tokenizer.TokenString         //quoted string
	TokenStringFragment     TokenType = tokenizer.TokenStringFragment //fragment framed (quoted) string
)

const (
	/*
	 * Query Statements
	 */
	TokenSelect TokenType = iota + 1
	TokenInsert
	TokenUpdate
	TokenDelete
	/*
	 * Clauses
	 */
	TokenAs
	TokenFrom
	TokenWhere
	TokenOrderBy
	TokenIn
	TokenValues
	/*
	 * Other tokens
	 */
	TokenAsterisk
	TokenPlus
	TokenDash
	TokenSlash
	TokenPercentage
	TokenComma
	TokenExprOpen
	TokenExprClose
	TokenStmtEnd
	TokenComment
	/*
	 * Comparisons
	 */
	TokenEq
	TokenNotEq
	TokenGt
	TokenLt
	TokenGte
	TokenLte
	/**
	 * Conditions
	 */
	TokenAnd
	TokenOr
	/*
	 * Default types
	 */
	TokenSingleQuotedString
	TokenLiteralString
	TokenDateParser
	TokenIntParser
	TokenFloatParser
)

type SqlTokenizer interface {
	Parse(query string) Tokens
}

func NewSqlTokenizer() SqlTokenizer {
	t := tokenizer.New()
	t.StopOnUndefinedToken() // TODO: maybe use tokenizer.TokenUndef to cut comments
	t.AllowKeywordSymbols(tokenizer.Underscore, append(tokenizer.Numbers, '.'))
	// Query Identifiers
	t.DefineTokens(TokenSelect, []string{"SELECT"})
	t.DefineTokens(TokenInsert, []string{"INSERT INTO"})
	t.DefineTokens(TokenUpdate, []string{"UPDATE"})
	t.DefineTokens(TokenDelete, []string{"DELETE"})
	// Clauses
	//t.DefineTokens(TokenAs, []string{"AS"})
	t.DefineTokens(TokenFrom, []string{"FROM"})
	t.DefineTokens(TokenWhere, []string{"WHERE"})
	t.DefineTokens(TokenOrderBy, []string{"ORDER BY"})
	//t.DefineTokens(TokenIn, []string{"IN"})
	t.DefineTokens(TokenValues, []string{"VALUES"})
	// Other tokens
	t.DefineTokens(TokenAsterisk, []string{"*"})
	//t.DefineTokens(TokenPlus, []string{"+"})
	//t.DefineTokens(TokenDash, []string{"-"})
	//t.DefineTokens(TokenSlash, []string{"/"})
	//t.DefineTokens(TokenPercentage, []string{"%"})
	t.DefineTokens(TokenComma, []string{","})
	t.DefineTokens(TokenExprOpen, []string{"("})
	t.DefineTokens(TokenExprClose, []string{")"})
	//t.DefineTokens(TokenStmtEnd, []string{";"})
	//t.DefineTokens(TokenComment, []string{"--"})
	// Comparisons
	t.DefineTokens(TokenEq, []string{"="})
	t.DefineTokens(TokenNotEq, []string{"!="})
	t.DefineTokens(TokenGt, []string{">"})
	t.DefineTokens(TokenLt, []string{"<"})
	t.DefineTokens(TokenGte, []string{">="})
	t.DefineTokens(TokenLte, []string{"<="})
	// Conditions
	t.DefineTokens(TokenAnd, []string{"AND"})
	t.DefineTokens(TokenOr, []string{"OR"})
	// Default Types
	t.DefineStringToken(TokenDoubleQuotedString, "\"", "\"").SetEscapeSymbol(tokenizer.BackSlash)
	t.DefineStringToken(TokenSingleQuotedString, "'", "'").SetEscapeSymbol(tokenizer.BackSlash)
	t.DefineStringToken(TokenLiteralString, "`", "`").SetEscapeSymbol(tokenizer.BackSlash)
	//t.DefineTokens(TokenDateParser, []string{"DATE"})
	//t.DefineTokens(TokenIntParser, []string{"INT"})
	//t.DefineTokens(TokenFloatParser, []string{"FLOAT"})
	return &sqlTokenizer{t}
}

type sqlTokenizer struct {
	*tokenizer.Tokenizer
}

func (q *sqlTokenizer) Parse(query string) Tokens {
	stream := q.ParseString(query)
	defer stream.Close()

	tokens := make([]*Token, 0)
	for stream.IsValid() {
		ct := stream.CurrentToken()
		tokens = append(tokens, &Token{
			Type:  ct.Key(),
			Value: ct.ValueString(),
		})
		stream.GoNext()
	}

	return newTokens(tokens)
}
