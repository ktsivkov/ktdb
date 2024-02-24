package data_test

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"ktdb/pkg/data"
)

func TestColumnSchema_Bytes__and__LoadColumnSchemaFromBytes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("with no default", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, bytes)
			assert.NoError(t, err)
			assert.Equal(t, original, restored)
		})
		t.Run("with default", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    ColMock(0xFF),
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			processorMock.On("FromReflectionType", reflect.TypeOf(ColMock(0x00)), 5, []byte{0xFF, 0x00, 0x00, 0x00, 0x00}).Return(ColMock(0xFF), nil)
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, bytes)
			assert.NoError(t, err)
			assert.Equal(t, original, restored)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("loading type error", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(nil, errors.New("error"))
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, bytes)
			assert.EqualError(t, err, "loading type failed: error")
			assert.Nil(t, restored)
		})
		t.Run("fail upon default marshalling", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    ColMock(0xF0),
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			bytes, err := original.Bytes()
			assert.EqualError(t, err, "marshalling of default failed: (column=[name=test-col]) marshal failed: error")
			assert.Nil(t, bytes)
		})
		t.Run("fail upon default unmarshalling", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    ColMock(0x0F),
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			processorMock.On("FromReflectionType", reflect.TypeOf(ColMock(0x00)), 5, []byte{0xf, 0x00, 0x00, 0x00, 0x00}).Return(nil, errors.New("error"))
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, bytes)
			assert.EqualError(t, err, "unmarshalling default value failed: (column=[name=test-col]) unmarshal failed: error")
			assert.Nil(t, restored)
		})
		t.Run("corrupted payload", func(t *testing.T) {
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, []byte{})
			assert.EqualError(t, err, "corrupted payload")
			assert.Nil(t, restored)
		})
		t.Run("payload reading error", func(t *testing.T) {
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, []byte{0x00})
			assert.EqualError(t, err, "deserialization failed: payload has no size defined")
			assert.Nil(t, restored)
		})
	})
}

func TestColumnSchema_ByteSize(t *testing.T) {
	schema := data.ColumnSchema{
		Name:       "test-col",
		ColumnSize: 5,
		Nullable:   false,
		Default:    nil,
		Type:       nil,
	}
	assert.Equal(t, schema.ByteSize(), 6)
}

func TestColumnSchema_Validate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		schema := data.ColumnSchema{
			Name:       "test-col",
			ColumnSize: 5,
			Nullable:   false,
			Default:    nil,
			Type:       reflect.TypeOf(ColMock(0x00)),
		}
		assert.NoError(t, schema.Validate(ColMock(0x00)))
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("null on a non nullable", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			assert.EqualError(t, schema.Validate(nil), "(column=[name=test-col]) is not nullable")
		})
		t.Run("not the same type", func(t *testing.T) {
			type differentColMock struct {
				ColMock
			}
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			assert.EqualError(t, schema.Validate(differentColMock{ColMock(0x00)}), "(column=[name=test-col]) given type [name=col_mock, type=data_test.differentColMock] doesn't match required type [name=col_mock, type=data_test.ColMock]")
		})
	})
}

func TestColumnSchema_Marshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("null on nullable", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   true,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			res, err := schema.Marshal(nil)
			assert.NoError(t, err)
			assert.Equal(t, res, make([]byte, 6))
		})
		t.Run("successful marshaling", func(t *testing.T) {
			expected := make([]byte, 6)
			expected[0], expected[1] = 0xFF, 0xFF
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   true,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			res, err := schema.Marshal(ColMock(0xFF))
			assert.NoError(t, err)
			assert.Equal(t, expected, res)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("validation error", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			res, err := schema.Marshal(nil)
			assert.EqualError(t, err, "validation failed: (column=[name=test-col]) is not nullable")
			assert.Nil(t, res)
		})
		t.Run("error on marshal", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			res, err := schema.Marshal(ColMock(0x00))
			assert.EqualError(t, err, "(column=[name=test-col]) marshal failed: error")
			assert.Nil(t, res)
		})
	})
}

func TestColumnSchema_Unmarshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("null on nullable", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   true,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			processorMock := &ColumnProcessorMock{}
			processorMock.On("FromReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			res, err := schema.Unmarshal(processorMock, make([]byte, 6))
			assert.NoError(t, err)
			assert.Nil(t, res)
		})
		t.Run("successful unmarshalling", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			given := make([]byte, 6)
			given[0], given[1] = 0xFF, 0xFF
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			processorMock.On("FromReflectionType", reflect.TypeOf(ColMock(0x00)), 5, []byte{0xFF, 0x00, 0x00, 0x00, 0x00}).Return(ColMock(0xFF), nil)
			res, err := schema.Unmarshal(processorMock, given)
			assert.NoError(t, err)
			assert.Equal(t, ColMock(0xFF), res)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("wrong payload size", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 6,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			res, err := schema.Unmarshal(processorMock, make([]byte, 6))
			assert.EqualError(t, err, "(column=[name=test-col]) corrupted data, payload size [size=6] differs than the expected [size=7]")
			assert.Nil(t, res)
		})
		t.Run("null on not-nullable", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			res, err := schema.Unmarshal(processorMock, make([]byte, 6))
			assert.EqualError(t, err, "(column=[name=test-col]) corrupted data, cannot assign null on a not-nullable column")
			assert.Nil(t, res)
		})
		t.Run("error on unmarshal", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(ColMock(0x00)),
			}
			given := make([]byte, 6)
			given[0] = 0xFF
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
			processorMock.On("FromReflectionType", reflect.TypeOf(ColMock(0x00)), 5, []byte{0x00, 0x00, 0x00, 0x00, 0x00}).Return(nil, errors.New("error"))
			res, err := schema.Unmarshal(processorMock, given)
			assert.EqualError(t, err, "(column=[name=test-col]) unmarshal failed: error")
			assert.Nil(t, res)
		})
	})
}

func TestColumnSchema_MarshalAndUnmarshal(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		schema := data.ColumnSchema{
			Name:       "test-col",
			ColumnSize: 5,
			Nullable:   true,
			Default:    nil,
			Type:       reflect.TypeOf(ColMock(0x00)),
		}
		given := data.Column(nil)
		res, err := schema.Marshal(given)
		assert.NoError(t, err)
		processorMock := &ColumnProcessorMock{}
		processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
		unmarshalRes, err := schema.Unmarshal(processorMock, res)
		assert.NoError(t, err)
		assert.Equal(t, given, unmarshalRes)
	})
	t.Run("non-nil", func(t *testing.T) {
		schema := data.ColumnSchema{
			Name:       "test-col",
			ColumnSize: 5,
			Nullable:   true,
			Default:    nil,
			Type:       reflect.TypeOf(ColMock(0x00)),
		}
		given := data.Column(ColMock(0xFF))
		res, err := schema.Marshal(given)
		assert.NoError(t, err)
		processorMock := &ColumnProcessorMock{}
		processorMock.On("ReflectionType", ColMock(0x00).Type(0)).Return(reflect.TypeOf(ColMock(0x00)), nil)
		processorMock.On("FromReflectionType", reflect.TypeOf(ColMock(0x00)), 5, []byte{0xFF, 0x00, 0x00, 0x00, 0x00}).Return(ColMock(0xFF), nil)
		unmarshalRes, err := schema.Unmarshal(processorMock, res)
		assert.NoError(t, err)
		assert.Equal(t, given, unmarshalRes)
	})
}
