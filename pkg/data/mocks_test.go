package data_test

import (
	"errors"

	"ktdb/pkg/data"
)

type ColMock byte

func (c ColMock) TypeName(_ int) string {
	return "col_mock"
}

func (c ColMock) Marshal(size int) ([]byte, error) {
	if c == 0x00 {
		return nil, errors.New("error")
	}
	res := make([]byte, size)
	copy(res, []byte{byte(c)})
	return res, nil
}

func (c ColMock) Unmarshal(_ int, payload []byte) (data.Column, error) {
	if len(payload) != 1 {
		return nil, errors.New("error")
	}

	return ColMock(payload[0]), nil
}
