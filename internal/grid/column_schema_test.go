package grid_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/internal/grid"
)

func TestColumnSchema_ByteSize(t *testing.T) {
	schema := grid.ColumnSchema{
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
		schema := grid.ColumnSchema{
			Name:       "test-col",
			ColumnSize: 5,
			Nullable:   false,
			Default:    nil,
			Type:       reflect.TypeOf(TestByteColMock(0x00)),
		}
		assert.NoError(t, schema.Validate(TestByteColMock(0x00)))
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("null on a non nullable", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			assert.EqualError(t, schema.Validate(nil), "(column=[name=test-col]) is not nullable")
		})
		t.Run("not the same type", func(t *testing.T) {
			type differentColMock struct {
				TestByteColMock
			}
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			assert.EqualError(t, schema.Validate(differentColMock{TestByteColMock(0x00)}), "(column=[name=test-col]) given type [name=col_mock, type=grid_test.differentColMock] doesn't match required type [name=col_mock, type=grid_test.TestByteColMock]")
		})
	})
}

func TestColumnSchema_Marshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("null on nullable", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   true,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			res, err := schema.Marshal(nil)
			assert.NoError(t, err)
			assert.Equal(t, res, make([]byte, 6))
		})
		t.Run("successful marshaling", func(t *testing.T) {
			expected := make([]byte, 6)
			expected[0], expected[1] = 0xFF, 0xFF
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   true,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			res, err := schema.Marshal(TestByteColMock(0xFF))
			assert.NoError(t, err)
			assert.Equal(t, expected, res)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("validation error", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			res, err := schema.Marshal(nil)
			assert.EqualError(t, err, "validation failed: (column=[name=test-col]) is not nullable")
			assert.Nil(t, res)
		})
		t.Run("error on marshal", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			res, err := schema.Marshal(TestByteColMock(0x00))
			assert.EqualError(t, err, "(column=[name=test-col]) marshal failed: error")
			assert.Nil(t, res)
		})
	})
}

func TestColumnSchema_Unmarshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("null on nullable", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   true,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			res, err := schema.Unmarshal(make([]byte, 6))
			assert.NoError(t, err)
			assert.Nil(t, res)
		})
		t.Run("successful unmarshalling", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			given := make([]byte, 6)
			given[0], given[1] = 0xFF, 0xFF
			res, err := schema.Unmarshal(given)
			assert.NoError(t, err)
			assert.Equal(t, TestByteColMock(0xFF), res)
		})
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("wrong payload size", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 6,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			res, err := schema.Unmarshal(make([]byte, 6))
			assert.EqualError(t, err, "(column=[name=test-col]) corrupted data, payload size [size=6] differs than the expected [size=7]")
			assert.Nil(t, res)
		})
		t.Run("null on not-nullable", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			res, err := schema.Unmarshal(make([]byte, 6))
			assert.EqualError(t, err, "(column=[name=test-col]) corrupted data, cannot assign null on a not-nullable column")
			assert.Nil(t, res)
		})
		t.Run("error on unmarshal", func(t *testing.T) {
			schema := grid.ColumnSchema{
				Name:       "test-col",
				ColumnSize: 5,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			}
			given := make([]byte, 6)
			given[0] = 0xFF
			res, err := schema.Unmarshal(given)
			assert.EqualError(t, err, "(column=[name=test-col]) unmarshal failed: cannot parse column: error")
			assert.Nil(t, res)
		})
	})
}
