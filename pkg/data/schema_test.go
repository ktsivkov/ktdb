package data_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/data"
	"ktdb/pkg/engine"
)

func TestRowSchema_Bytes__and__LoadRowSchemaFromBytes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("a non-empty row schema", func(t *testing.T) {
			original, err := data.NewRowSchema([]*data.ColumnSchema{
				{
					Type:       ColMock{}.TypeIdentifier(),
					Default:    nil,
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
				},
			})
			assert.NoError(t, err)
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
			res, err := data.LoadRowSchemaFromBytes(processorMock, bytes)
			assert.NoError(t, err)
			assert.Equal(t, original, res)
		})
		t.Run("empty row schema", func(t *testing.T) {
			original, err := data.NewRowSchema([]*data.ColumnSchema{})
			assert.NoError(t, err)
			bytes, err := original.Bytes()
			assert.NoError(t, err)
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(nil, nil)
			res, err := data.LoadRowSchemaFromBytes(processorMock, bytes)
			assert.NoError(t, err)
			assert.Equal(t, original, res)
		})
		t.Run("restore from empty payload", func(t *testing.T) {
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(nil, nil)
			res, err := data.LoadRowSchemaFromBytes(processorMock, []byte{})
			assert.EqualError(t, err, "corrupted payload")
			assert.Nil(t, res)
		})
	})
}

func TestNewRowSchema(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		res, err := data.NewRowSchema([]*data.ColumnSchema{
			{
				Name:       "test-col-1",
				ColumnSize: 1,
				Nullable:   false,
				Default:    nil,
				Type:       ColMock{}.TypeIdentifier(),
			},
			{
				Name:       "test-col-2",
				ColumnSize: 1,
				Nullable:   false,
				Default:    nil,
				Type:       ColMock{}.TypeIdentifier(),
			},
		})
		assert.NoError(t, err)
		assert.IsType(t, &data.RowSchema{}, res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("duplicate column", func(t *testing.T) {
			res, err := data.NewRowSchema([]*data.ColumnSchema{
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       ColMock{}.TypeIdentifier(),
				},
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       ColMock{}.TypeIdentifier(),
				},
			})
			assert.EqualError(t, err, "(row=[column_position=1, column_name=test-col]) already exists")
			assert.Nil(t, res)
		})
		t.Run("not defined", func(t *testing.T) {
			res, err := data.NewRowSchema([]*data.ColumnSchema{
				nil,
			})
			assert.EqualError(t, err, "(row=[column_position=0]) is not defined")
			assert.Nil(t, res)
		})
		t.Run("missing a name", func(t *testing.T) {
			res, err := data.NewRowSchema([]*data.ColumnSchema{
				{
					Name:       "",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       ColMock{}.TypeIdentifier(),
				},
			})
			assert.EqualError(t, err, "(row=[column_position=0]) is missing a name")
			assert.Nil(t, res)
		})
		t.Run("missing a type", func(t *testing.T) {
			res, err := data.NewRowSchema([]*data.ColumnSchema{
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       "",
				},
			})
			assert.EqualError(t, err, "(row=[column_position=0, column_name=test-col]) is missing a type")
			assert.Nil(t, res)
		})
		t.Run("default value type mismatch", func(t *testing.T) {
			res, err := data.NewRowSchema([]*data.ColumnSchema{
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    InvalidColMock{},
					Type:       ColMock{}.TypeIdentifier(),
				},
			})
			assert.EqualError(t, err, "(row=[column_position=0, column_name=test-col]) default value validation failed: (column=[name=test-col]) given type [name=] doesn't match required type [name=col_mock]")
			assert.Nil(t, res)
		})
	})
}

func getRowSchema(typeIdentifier string, defaultVal engine.Column) *data.RowSchema {
	res, _ := data.NewRowSchema([]*data.ColumnSchema{
		{
			Name:       "test-col-1",
			ColumnSize: 1,
			Nullable:   false,
			Default:    nil,
			Type:       typeIdentifier,
		},
		{
			Name:       "test-col-2",
			ColumnSize: 1,
			Nullable:   true,
			Default:    nil,
			Type:       typeIdentifier,
		},
		{
			Name:       "test-col-3",
			ColumnSize: 1,
			Nullable:   true,
			Default:    defaultVal,
			Type:       typeIdentifier,
		},
	})
	return res
}

func TestRowSchema_Prepare(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expected := []engine.Column{
			ColMock{},
			nil,
			ColMock{},
		}
		schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
		res, err := schema.Prepare(map[string]engine.Column{
			"test-col-1": ColMock{},
		})
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("validation fail", func(t *testing.T) {
			schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
			res, err := schema.Prepare(map[string]engine.Column{})
			assert.EqualError(t, err, "validation failed: (column=[name=test-col-1]) is not nullable")
			assert.Nil(t, res)
		})
	})
}

func TestRowSchema_Row(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
		res, err := schema.Row([]engine.Column{
			ColMock{},
			nil,
			ColMock{},
		})
		assert.NoError(t, err)
		assert.Len(t, res, schema.ByteSize())
	})

	t.Run("fail", func(t *testing.T) {
		t.Run("wrong column number", func(t *testing.T) {
			schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
			res, err := schema.Row([]engine.Column{})
			assert.EqualError(t, err, "expected columns [size=0], got [size=3]")
			assert.Nil(t, res)
		})
		t.Run("marshal error", func(t *testing.T) {
			schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
			res, err := schema.Row([]engine.Column{
				ColMockMarshalFail{},
				nil,
				ColMock{},
			})
			assert.EqualError(t, err, "could not marshal column: (column=[name=test-col-1]) marshal failed: error")
			assert.Nil(t, res)
		})
	})
}

func TestRowSchema_Columns(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
		processorMock := &ColumnProcessorMock{}
		processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(reflect.TypeOf(ColMock{}), nil)
		processorMock.On("FromType", ColMock{}.TypeIdentifier(), 1, []byte{0xFF}).Return(ColMock{}, nil)
		res, err := schema.Columns(processorMock, data.Row{
			// NullFlag, Value
			0xFF, 0xFF,
			0x00, 0x00,
			0xFF, 0xFF,
		})
		assert.NoError(t, err)
		assert.Equal(t, []engine.Column{
			ColMock{},
			nil,
			ColMock{},
		}, res)
	})

	t.Run("fail", func(t *testing.T) {
		t.Run("wrong size of row", func(t *testing.T) {
			schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
			processorMock := &ColumnProcessorMock{}
			res, err := schema.Columns(processorMock, nil)
			assert.EqualError(t, err, "expected row of size [bytes=6], got [bytes=0]")
			assert.Nil(t, res)
		})
		t.Run("unmarshal error", func(t *testing.T) {
			processorMock := &ColumnProcessorMock{}
			processorMock.On("ReflectionType", ColMock{}.Type(0)).Return(nil, nil)
			schema := getRowSchema(ColMock{}.TypeIdentifier(), ColMock{})
			res, err := schema.Columns(processorMock, data.Row{
				// NullFlag, Value
				0x00, 0x00,
				0x00, 0x00,
				0xFF, 0xFF,
			})
			assert.EqualError(t, err, "failed unmarshalling column: (column=[name=test-col-1]) corrupted data, cannot assign null on a not-nullable column")
			assert.Nil(t, res)
		})
	})
}
