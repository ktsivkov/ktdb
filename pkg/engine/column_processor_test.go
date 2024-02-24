package engine_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/engine"
)

func TestNewColumnProcessor(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		res, err := engine.NewColumnProcessor([]reflect.Type{
			reflect.TypeOf(ColMock{}),
		})
		assert.NoError(t, err)
		assert.Implements(t, new(engine.ColumnProcessor), res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("empty identifier", func(t *testing.T) {
			res, err := engine.NewColumnProcessor([]reflect.Type{
				reflect.TypeOf(InvalidColMock{}),
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=engine_test.InvalidColMock]) invalid identifier")
			assert.Nil(t, res)
		})
		t.Run("already registered identifier", func(t *testing.T) {
			res, err := engine.NewColumnProcessor([]reflect.Type{
				reflect.TypeOf(ColMock{}),
				reflect.TypeOf(ColMock{}),
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=engine_test.ColMock, identifier=col_mock]) identifier already used")
			assert.Nil(t, res)
		})
		t.Run("empty type", func(t *testing.T) {
			res, err := engine.NewColumnProcessor([]reflect.Type{
				nil,
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=nil]) is not a valid type")
			assert.Nil(t, res)
		})
		t.Run("invalid type", func(t *testing.T) {
			res, err := engine.NewColumnProcessor([]reflect.Type{
				reflect.TypeOf(""),
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=string]) does not implement [type=data.Column]")
			assert.Nil(t, res)
		})
	})
}

func TestTypes_ReflectionType(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cm := ColMock{}
		types, _ := engine.NewColumnProcessor([]reflect.Type{
			reflect.TypeOf(cm),
		})
		res, err := types.ReflectionType(cm.Identifier())
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(cm), res)
	})
	t.Run("fail", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]reflect.Type{})
		res, err := types.ReflectionType("not-existent")
		assert.EqualError(t, err, "(type=[identifier=not-existent]) not found")
		assert.Nil(t, res)
	})
}

func TestColumnProcessor_FromReflectionType(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]reflect.Type{})
		given := []byte{0xFF}
		expected := &ColMock{}

		res, err := types.FromReflectionType(reflect.TypeOf(ColMock{}), 1, given)

		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("fail - type mismatch", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]reflect.Type{})
		given := reflect.TypeOf(interface{}(""))
		_, err := types.FromReflectionType(given, 1, make([]byte, 0))
		assert.EqualError(t, err, "invalid column type [string]")
	})

	t.Run("fail - invalid payload", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]reflect.Type{})
		_, err := types.FromReflectionType(reflect.TypeOf(InvalidColMock{}), 1, nil)
		assert.EqualError(t, err, "cannot parse column: error")
	})
}
