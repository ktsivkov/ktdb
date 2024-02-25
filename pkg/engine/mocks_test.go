package engine_test

import (
	"github.com/pkg/errors"

	"ktdb/pkg/engine"
)

type ColMock struct{}

func (c *ColMock) Bytes(int) ([]byte, error) {
	return []byte{0xFF}, nil
}

func (c *ColMock) Type() engine.ColumnType {
	return "col-mock"
}

type ColMockProcessor struct{}

func (c *ColMockProcessor) Type() engine.ColumnType {
	return "col-mock"
}

func (c *ColMockProcessor) Load(_ int, payload []byte) (engine.Column, error) {
	if payload == nil {
		return nil, errors.New("error")
	}
	return &ColMock{}, nil
}

type InvalidColMock struct{}

func (c *InvalidColMock) Bytes(int) ([]byte, error) {
	return nil, errors.New("error")
}

func (c *InvalidColMock) Type() engine.ColumnType {
	return ""
}

type InvalidColMockProcessor struct{}

func (c *InvalidColMockProcessor) Type() engine.ColumnType {
	return ""
}

func (c *InvalidColMockProcessor) Load(_ int, payload []byte) (engine.Column, error) {
	return nil, errors.New("error")
}
