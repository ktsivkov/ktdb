package data_test

import (
	"errors"

	"ktdb/pkg/data"
)

type InvalidColMock struct{}

func (i InvalidColMock) Identifier() string {
	return ""
}

func (i InvalidColMock) Type(_ int) string {
	return ""
}

func (i InvalidColMock) Marshal(_ int) ([]byte, error) {
	return nil, nil
}

func (i InvalidColMock) Unmarshal(_ int, _ []byte) (data.Column, error) {
	return &InvalidColMock{}, nil
}

type ColMock byte

func (c ColMock) Identifier() string {
	return "col_mock"
}

func (c ColMock) Type(_ int) string {
	return "col_mock"
}

func (c ColMock) Marshal(size int) ([]byte, error) {
	switch c {
	case 0x00, 0xF0:
		return nil, errors.New("error")
	default:
		res := make([]byte, size)
		copy(res, []byte{byte(c)})
		return res, nil
	}
}

func (c ColMock) Unmarshal(_ int, payload []byte) (data.Column, error) {
	if payload == nil || len(payload) == 0 {
		return nil, errors.New("error")
	}
	switch payload[0] {
	case 0x00, 0x0F:
		return nil, errors.New("error")
	default:
		return ColMock(payload[0]), nil
	}
}
