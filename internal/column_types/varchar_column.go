package column_types

import (
	"fmt"
	"unicode/utf8"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
	"ktdb/pkg/sys"
)

type Varchar string

func (v Varchar) TypeName(size int) string {
	return fmt.Sprintf("varchar[size=%d]", size)
}

func (v Varchar) Unmarshal(size int, payload []byte) (data.Column, error) {
	if utf8.Valid(payload) == false {
		return nil, errors.Errorf("(%s) payload bytes are not valid UTF-8", v.TypeName(size))
	}

	return Varchar(sys.RemovePadding(payload)), nil
}

func (v Varchar) Marshal(size int) ([]byte, error) {
	payload := []byte(v)
	if len(payload) > size {
		return nil, errors.Errorf("(%s) data exceeds maximum size", v.TypeName(size))
	}

	return sys.AddPadding(payload, size)
}
