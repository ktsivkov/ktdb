package column_types

import (
	"unicode/utf8"

	"github.com/pkg/errors"

	"ktdb/pkg/data"
	"ktdb/pkg/payload"
)

type Str string

func (s Str) TypeName() string {
	return "string"
}

func (s Str) Unmarshal(payload payload.Payload) (data.Column, error) {
	var err error
	payload, _, err = payload.Read()
	if err != nil {
		return nil, errors.Wrapf(err, "(%s) invalid payload", s.TypeName())
	}

	if utf8.Valid(payload) == false {
		return nil, errors.Errorf("(%s) payload bytes not valid UTF-8", s.TypeName())
	}

	return Str(payload), nil
}

func (s Str) Marshal() (payload.Payload, error) {
	return payload.New([]byte(s))
}
