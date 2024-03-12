package tokenizer

type Cond func(token *Token) bool

func IsType(typ TokenType) Cond {
	return func(token *Token) bool {
		return token.Type == typ
	}
}

func Is(val string, caseSensitive bool) Cond {
	return func(token *Token) bool {
		return token.Is(val, caseSensitive)
	}
}

func CondGroup(conditions ...Cond) Cond {
	return func(token *Token) bool {
		for _, cond := range conditions {
			if !cond(token) {
				return false
			}
		}
		return true
	}
}
