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
			assert.EqualError(t, err, "type registration failed: (type=[type=string]) does not implement [type=engine.Column]")
			assert.Nil(t, res)
		})
	})
}

func TestColumnProcessor_FromType(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]reflect.Type{
			reflect.TypeOf(ColMock{}),
		})
		given := []byte{0xFF}
		expected := &ColMock{}

		res, err := types.FromType(ColMock{}.TypeIdentifier(), 1, given)

		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("fail - invalid payload", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]reflect.Type{
			reflect.TypeOf(ColMock{}),
		})
		_, err := types.FromType(ColMock{}.TypeIdentifier(), 1, nil)
		assert.EqualError(t, err, "cannot parse column: error")
	})

	t.Run("fail - invalid type", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]reflect.Type{
			reflect.TypeOf(ColMock{}),
		})
		_, err := types.FromType(InvalidColMock{}.TypeIdentifier(), 1, nil)
		assert.EqualError(t, err, "unable to get reflection type: (type=[identifier=]) not found")
	})
}
