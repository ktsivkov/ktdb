package engine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/engine"
)

func TestNewColumnProcessor(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		res, err := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
			&ColMockProcessor{},
		})
		assert.NoError(t, err)
		assert.Implements(t, new(engine.ColumnProcessor), res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("empty identifier", func(t *testing.T) {
			res, err := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
				&InvalidColMockProcessor{},
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=*engine_test.InvalidColMockProcessor]) invalid identifier")
			assert.Nil(t, res)
		})
		t.Run("already registered identifier", func(t *testing.T) {
			res, err := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
				&ColMockProcessor{},
				&ColMockProcessor{},
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=*engine_test.ColMockProcessor, identifier=col-mock]) identifier already used")
			assert.Nil(t, res)
		})
		t.Run("empty type", func(t *testing.T) {
			res, err := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
				nil,
			})
			assert.EqualError(t, err, "type registration failed: (type=[type=nil]) is not a valid type")
			assert.Nil(t, res)
		})
	})
}

func TestColumnProcessor_FromType(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
			&ColMockProcessor{},
		})
		given := []byte{0xFF}
		expected := &ColMock{}

		res, err := types.FromType(expected.Type(), 1, given)

		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("fail - invalid payload", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
			&ColMockProcessor{},
		})
		col := &ColMock{}
		_, err := types.FromType(col.Type(), 1, nil)
		assert.EqualError(t, err, "cannot parse column: error")
	})

	t.Run("fail - invalid type", func(t *testing.T) {
		types, _ := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
			&ColMockProcessor{},
		})
		col := &InvalidColMock{}
		_, err := types.FromType(col.Type(), 1, nil)
		assert.EqualError(t, err, "unable to load processor: (processor=[identifier=]) not found")
	})
}
