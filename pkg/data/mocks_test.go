package data_test

import (
	"errors"

	"ktdb/pkg/data"
	"ktdb/pkg/payload"
)

type ColMock byte

func (c ColMock) TypeName() string {
	return "col_mock"
}

func (c ColMock) Marshal() (payload.Payload, error) {
	if c == 0x00 {
		return nil, errors.New("error")
	}

	return []byte{byte(c)}, nil
}

func (c ColMock) Unmarshal(payload payload.Payload) (data.Column, error) {
	if len(payload) != 1 {
		return nil, errors.New("error")
	}

	return ColMock(payload[0]), nil
}
