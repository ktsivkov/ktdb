package data_test

import (
	"errors"
	"reflect"

	"github.com/stretchr/testify/mock"

	"ktdb/pkg/data"
)

type ColumnProcessorMock struct {
	mock.Mock
}

func (c *ColumnProcessorMock) ReflectionType(identifier string) (reflect.Type, error) {
	args := c.Called(identifier)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(reflect.Type), args.Error(1)
}

func (c *ColumnProcessorMock) FromReflectionType(columnType reflect.Type, size int, payload []byte) (data.Column, error) {
	args := c.Called(columnType, size, payload)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(data.Column), args.Error(1)
}

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
