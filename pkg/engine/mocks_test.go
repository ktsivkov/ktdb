package engine_test

import (
	"github.com/pkg/errors"

	"ktdb/pkg/data"
)

type InvalidColMock struct{}

func (i InvalidColMock) Identifier() string {
	return ""
}

func (i InvalidColMock) Type(int) string {
	return ""
}

func (i InvalidColMock) Marshal(int) ([]byte, error) {
	return nil, errors.New("error")
}

func (i InvalidColMock) Unmarshal(int, []byte) (data.Column, error) {
	return nil, errors.New("error")
}

type ColMock struct{}

func (c ColMock) Identifier() string {
	return "col_mock"
}

func (c ColMock) Type(int) string {
	return "col_mock"
}

func (c ColMock) Marshal(int) ([]byte, error) {
	return []byte{0xFF}, nil
}

func (c ColMock) Unmarshal(int, []byte) (data.Column, error) {
	return &ColMock{}, nil
}
