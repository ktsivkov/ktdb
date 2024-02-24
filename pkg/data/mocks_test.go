package data_test

import (
	"errors"
	"reflect"

	"github.com/stretchr/testify/mock"

	"ktdb/pkg/engine"
)

type ColumnProcessorMock struct {
	mock.Mock
}

func (c *ColumnProcessorMock) ReflectionType(typeIdentifier string) (reflect.Type, error) {
	args := c.Called(typeIdentifier)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(reflect.Type), args.Error(1)
}

func (c *ColumnProcessorMock) FromType(typeIdentifier string, size int, payload []byte) (engine.Column, error) {
	args := c.Called(typeIdentifier, size, payload)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(engine.Column), args.Error(1)
}

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

func (c ColMock) Unmarshal(int, []byte) (engine.Column, error) {
	return &ColMock{}, nil
}

type ColMockMarshalFail struct {
	ColMock
}

func (c ColMockMarshalFail) Marshal(int) ([]byte, error) {
	return nil, errors.New("error")
}
