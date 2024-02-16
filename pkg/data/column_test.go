package data_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/data"
)

func TestColumnFromType(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		given := []byte{0x01}
		expected := ColMock(0x01)

		res, err := data.ColumnFromType(reflect.TypeOf(ColMock(0)), 1, given)

		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("fail - type mismatch", func(t *testing.T) {
		given := reflect.TypeOf(interface{}(""))
		_, err := data.ColumnFromType(given, 1, make([]byte, 0))
		assert.EqualError(t, err, "invalid column type [string]")
	})

	t.Run("fail - invalid payload", func(t *testing.T) {
		_, err := data.ColumnFromType(reflect.TypeOf(ColMock(0x00)), 1, nil)
		assert.EqualError(t, err, "cannot parse column: error")
	})
}
