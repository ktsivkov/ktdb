package tokenizer

import (
	"github.com/bzick/tokenizer"
)

type TokenType = tokenizer.TokenKey

const (
	/*
	 * Allies for default Tokenizer tokens
	 */
	TokenUnknown            TokenType = tokenizer.TokenUnknown        //unspecified token key.
	TokenKeyword            TokenType = tokenizer.TokenKeyword        //keyword, any combination of letters, including unicode letters.
	TokenInt                TokenType = tokenizer.TokenInteger        //integer value
	TokenFloat              TokenType = tokenizer.TokenFloat          //float/double value
	TokenDoubleQuotedString TokenType = tokenizer.TokenString         //quoted string
	TokenStringFragment     TokenType = tokenizer.TokenStringFragment //fragment framed (quoted) string
	/*
	 * Other tokens
	 */
	TokenAsterisk TokenType = iota + 1
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
	/*
	 * Default token types
	 */
	TokenSingleQuotedString
	TokenLiteralString
)

type SqlTokenizer interface {
	Parse(query string) Tokens
}

func NewSqlTokenizer() SqlTokenizer {
	t := tokenizer.New()
	t.StopOnUndefinedToken()                                                    // TODO: maybe use tokenizer.TokenUndef to cut comments
	t.AllowKeywordSymbols(tokenizer.Underscore, append(tokenizer.Numbers, '.')) // TODO: maybe set the dot as a separate token?
	// Other tokens
	t.DefineTokens(TokenAsterisk, []string{"*"})
	t.DefineTokens(TokenPlus, []string{"+"})
	t.DefineTokens(TokenDash, []string{"-"})
	t.DefineTokens(TokenSlash, []string{"/"})
	t.DefineTokens(TokenPercentage, []string{"%"})
	t.DefineTokens(TokenComma, []string{","})
	t.DefineTokens(TokenExprOpen, []string{"("})
	t.DefineTokens(TokenExprClose, []string{")"})
	t.DefineTokens(TokenStmtEnd, []string{";"})
	t.DefineTokens(TokenComment, []string{"--"})
	// Comparisons
	t.DefineTokens(TokenEq, []string{"="})
	t.DefineTokens(TokenNotEq, []string{"!="})
	t.DefineTokens(TokenGt, []string{">"})
	t.DefineTokens(TokenLt, []string{"<"})
	t.DefineTokens(TokenGte, []string{">="})
	t.DefineTokens(TokenLte, []string{"<="})
	// Default Types
	t.DefineStringToken(TokenDoubleQuotedString, "\"", "\"").SetEscapeSymbol(tokenizer.BackSlash)
	t.DefineStringToken(TokenSingleQuotedString, "'", "'").SetEscapeSymbol(tokenizer.BackSlash)
	t.DefineStringToken(TokenLiteralString, "`", "`").SetEscapeSymbol(tokenizer.BackSlash)
	return &sqlTokenizer{t}
}

type sqlTokenizer struct {
	*tokenizer.Tokenizer
}

func (q *sqlTokenizer) Parse(query string) Tokens {
	stream := q.ParseString(query)
	defer stream.Close()

	stack := make([]*Token, 0)
	for stream.IsValid() {
		ct := stream.CurrentToken()
		stack = append(stack, &Token{
			Type:  ct.Key(),
			Value: ct.ValueString(),
		})
		stream.GoNext()
	}

	return &tokens{
		stack: stack,
		len:   len(stack),
	}
}
