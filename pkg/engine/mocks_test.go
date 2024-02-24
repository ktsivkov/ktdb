package engine_test

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine"
)

type InvalidColMock struct{}

func (i InvalidColMock) TypeIdentifier() string {
	return ""
}

func (i InvalidColMock) Type(int) string {
	return ""
}

func (i InvalidColMock) Marshal(int) ([]byte, error) {
	return nil, errors.New("error")
}

func (i InvalidColMock) Unmarshal(int, []byte) (engine.Column, error) {
	return nil, errors.New("error")
}

type ColMock struct{}

func (c ColMock) TypeIdentifier() string {
	return "col_mock"
}

func (c ColMock) Type(int) string {
	return "col_mock"
}

func (c ColMock) Marshal(int) ([]byte, error) {
	return []byte{0xFF}, nil
}

func (c ColMock) Unmarshal(_ int, payload []byte) (engine.Column, error) {
	if payload == nil {
		return nil, errors.New("error")
	}
	return &ColMock{}, nil
}
