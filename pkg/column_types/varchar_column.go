package column_types

import (
	"fmt"
	"unicode/utf8"

	"github.com/pkg/errors"

	"ktdb/pkg/engine"
	"ktdb/pkg/sys"
)

type Varchar string

func (v Varchar) TypeIdentifier() string {
	return "varchar"
}

func (v Varchar) Type(size int) string {
	return fmt.Sprintf("%s[size=%d]", v.TypeIdentifier(), size)
}

func (v Varchar) Unmarshal(size int, payload []byte) (engine.Column, error) {
	if utf8.Valid(payload) == false {
		return nil, errors.Errorf("(%s) payload bytes are not valid UTF-8", v.Type(size))
	}

	return Varchar(sys.RemovePadding(payload)), nil
}

func (v Varchar) Marshal(size int) ([]byte, error) {
	payload := []byte(v)
	if len(payload) > size {
		return nil, errors.Errorf("(%s) data exceeds maximum size", v.Type(size))
	}

	return sys.AddPadding(payload, size)
}
