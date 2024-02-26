package column

import (
	"fmt"
	"unicode/utf8"

	"github.com/pkg/errors"
)

type Type string

func (t Type) Bytes() []byte {
	return []byte(t)
}

func (t Type) Load(payload []byte) (Type, error) {
	if utf8.Valid(payload) == false {
		return "", errors.Errorf("payload bytes are not valid UTF-8")
	}
	return Type(payload), nil
}

func (t Type) Format(size int) string {
	return fmt.Sprintf("%s[size=%d]", t, size)
}

func (t Type) Empty() bool {
	return t == ""
}

func (t Type) String() string {
	return string(t)
}

func (t Type) Equals(typ Type) bool {
	return t == typ
}
