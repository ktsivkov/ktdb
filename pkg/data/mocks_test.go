package data_test

import (
	"errors"

	"github.com/stretchr/testify/mock"

	"ktdb/pkg/engine"
)

type ColumnProcessorMock struct {
	mock.Mock
}

func (c *ColumnProcessorMock) FromType(typ engine.ColumnType, size int, payload []byte) (engine.Column, error) {
	args := c.Called(typ, size, payload)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(engine.Column), args.Error(1)
}

type ColMock struct{}

func (c *ColMock) Bytes(int) ([]byte, error) {
	return []byte{0xFF}, nil
}

func (c *ColMock) Type() engine.ColumnType {
	return "col-mock"
}

type ColMockBytesFail struct {
	ColMock
}

func (c *ColMockBytesFail) Bytes(int) ([]byte, error) {
	return nil, errors.New("error")
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
