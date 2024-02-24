package data_test

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"ktdb/pkg/data"
	"ktdb/pkg/engine"
)

func TestColumnSchema_Bytes__and__LoadColumnSchemaFromBytes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("with no default", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       ColMock{}.TypeIdentifier(),
			}
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, bytes)
			assert.NoError(t, err)
			assert.Equal(t, original, restored)
		})
		t.Run("with default", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    ColMock{},
				Type:       ColMock{}.TypeIdentifier(),
			}
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
			processorMock.On("FromType", ColMock{}.TypeIdentifier(), 5, []byte{0xFF, 0x00, 0x00, 0x00, 0x00}).Return(ColMock{}, nil)
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, bytes)
			assert.NoError(t, err)
			assert.Equal(t, original, restored)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("fail upon default marshalling", func(t *testing.T) {
			original := &data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    InvalidColMock{},
				Type:       InvalidColMock{}.TypeIdentifier(),
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
				Default:    ColMock{},
				Type:       ColMock{}.TypeIdentifier(),
			}
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(InvalidColMock{}), nil)
			processorMock.On("FromType", ColMock{}.TypeIdentifier(), 5, []byte{0xFF, 0x00, 0x00, 0x00, 0x00}).Return(nil, errors.New("error"))
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, bytes)
			assert.EqualError(t, err, "unmarshalling default value failed: (column=[name=test-col]) unmarshal failed: error")
			assert.Nil(t, restored)
		})
		t.Run("corrupted payload", func(t *testing.T) {
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
			restored, err := data.LoadColumnSchemaFromBytes(processorMock, []byte{})
			assert.EqualError(t, err, "corrupted payload")
			assert.Nil(t, restored)
		})
		t.Run("payload reading error", func(t *testing.T) {
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
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
		Type:       ColMock{}.TypeIdentifier(),
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
			Type:       ColMock{}.TypeIdentifier(),
		}
		assert.NoError(t, schema.ValidateColumn(ColMock{}))
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("null on a non nullable", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       ColMock{}.TypeIdentifier(),
			}
			assert.EqualError(t, schema.ValidateColumn(nil), "(column=[name=test-col]) is not nullable")
		})
		t.Run("not the same type", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       ColMock{}.TypeIdentifier(),
			}
			assert.EqualError(t, schema.ValidateColumn(InvalidColMock{}), "(column=[name=test-col]) given type [name=] doesn't match required type [name=col_mock]")
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
				Type:       ColMock{}.TypeIdentifier(),
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
				Type:       ColMock{}.TypeIdentifier(),
			}
			res, err := schema.Marshal(ColMock{})
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
				Type:       ColMock{}.TypeIdentifier(),
			}
			res, err := schema.Marshal(nil)
			assert.EqualError(t, err, "column validation failed: (column=[name=test-col]) is not nullable")
			assert.Nil(t, res)
		})
		t.Run("error on marshal", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       InvalidColMock{}.TypeIdentifier(),
			}
			res, err := schema.Marshal(InvalidColMock{})
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
				Type:       ColMock{}.TypeIdentifier(),
			}
			processorMock := &ColumnProcessorMock{}
			processorMock.On("FromType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
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
				Type:       ColMock{}.TypeIdentifier(),
			}
			given := make([]byte, 6)
			given[0], given[1] = 0xFF, 0xFF
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
			processorMock.On("FromType", ColMock{}.TypeIdentifier(), 5, []byte{0xFF, 0x00, 0x00, 0x00, 0x00}).Return(ColMock{}, nil)
			res, err := schema.Unmarshal(processorMock, given)
			assert.NoError(t, err)
			assert.Equal(t, ColMock{}, res)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("wrong payload size", func(t *testing.T) {
			schema := data.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 6,
				Nullable:   false,
				Default:    nil,
				Type:       ColMock{}.TypeIdentifier(),
			}
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
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
				Type:       ColMock{}.TypeIdentifier(),
			}
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
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
				Type:       ColMock{}.TypeIdentifier(),
			}
			given := make([]byte, 6)
			given[0] = 0xFF
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
			processorMock.On("FromType", ColMock{}.TypeIdentifier(), 5, []byte{0x00, 0x00, 0x00, 0x00, 0x00}).Return(nil, errors.New("error"))
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
			Type:       ColMock{}.TypeIdentifier(),
		}
		given := engine.Column(nil)
		res, err := schema.Marshal(given)
		assert.NoError(t, err)
		processorMock := &ColumnProcessorMock{}
		processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
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
			Type:       ColMock{}.TypeIdentifier(),
		}
		given := engine.Column(ColMock{})
		res, err := schema.Marshal(given)
		assert.NoError(t, err)
		processorMock := &ColumnProcessorMock{}
		processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
		processorMock.On("FromType", ColMock{}.TypeIdentifier(), 5, []byte{0xFF, 0x00, 0x00, 0x00, 0x00}).Return(ColMock{}, nil)
		unmarshalRes, err := schema.Unmarshal(processorMock, res)
		assert.NoError(t, err)
		assert.Equal(t, given, unmarshalRes)
	})
}
