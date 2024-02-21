package data_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/data"
)

func TestNewTypes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		res, err := data.NewTypes([]reflect.Type{
			reflect.TypeOf(ColMock(0x00)),
		})
		assert.NoError(t, err)
		assert.IsType(t, &data.Types{}, res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("empty identifier", func(t *testing.T) {
			res, err := data.NewTypes([]reflect.Type{
				reflect.TypeOf(InvalidColMock{}),
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=data_test.InvalidColMock]) invalid identifier")
			assert.Nil(t, res)
		})
		t.Run("already registered identifier", func(t *testing.T) {
			res, err := data.NewTypes([]reflect.Type{
				reflect.TypeOf(ColMock(0x00)),
				reflect.TypeOf(ColMock(0x00)),
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=data_test.ColMock, identifier=col_mock]) identifier already used")
			assert.Nil(t, res)
		})
		t.Run("empty type", func(t *testing.T) {
			res, err := data.NewTypes([]reflect.Type{
				nil,
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=nil]) is not a valid type")
			assert.Nil(t, res)
		})
		t.Run("invalid type", func(t *testing.T) {
			res, err := data.NewTypes([]reflect.Type{
				reflect.TypeOf(""),
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=string]) does not implement [type=data.Column]")
			assert.Nil(t, res)
		})
	})
}

func TestTypes_Get(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cm := ColMock(0x00)
		types, _ := data.NewTypes([]reflect.Type{
			reflect.TypeOf(cm),
		})
		res, err := types.Get(cm.Identifier())
		assert.NoError(t, err)
		assert.Equal(t, reflect.TypeOf(cm), res)
	})
	t.Run("fail", func(t *testing.T) {
		types, _ := data.NewTypes([]reflect.Type{})
		res, err := types.Get("not-existent")
		assert.EqualError(t, err, "(type=[identifier=not-existent]) not found")
		assert.Nil(t, res)
	})
}
