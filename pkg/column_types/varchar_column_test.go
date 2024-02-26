package column_types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/column_types"
)

func TestVarchar_Marshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Run("fail", func(t *testing.T) {
			givenData := "hello world"
			givenSize := int64(255)
			v := column_types.Varchar(givenData)
			res, err := v.Bytes(givenSize)
			assert.NoError(t, err)
			assert.Equal(t, givenSize, int64(len(res)))
			assert.Equal(t, []byte(givenData), res[:len(givenData)])                                     // Assert the data is the same removing the padding
			assert.Equal(t, make([]byte, givenSize-int64(len([]byte(givenData)))), res[len(givenData):]) // Assert the padding is empty bytes
		})
	})
	t.Run("fail", func(t *testing.T) {
		givenData := "hello world"
		givenSize := int64(1)
		v := column_types.Varchar(givenData)
		res, err := v.Bytes(givenSize)
		assert.EqualError(t, err, "(varchar[size=1]) data exceeds maximum size")
		assert.Nil(t, res)
	})
}

func TestVarcharProcessor_Load(t *testing.T) {
	t.Run("success - no padding", func(t *testing.T) {
		given := []byte("some-text")
		res, err := new(column_types.VarcharProcessor).Load(int64(len(given)), given)
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("success - padding", func(t *testing.T) {
		given := append([]byte("some-text"), make([]byte, 8)...)
		res, err := new(column_types.VarcharProcessor).Load(int64(len(given)), given)
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("fail", func(t *testing.T) {
		given := []byte{0xC3, 0x28, 0x3F, 0xE2, 0x82} // Invalid UTF-8 sequence of bytes
		res, err := new(column_types.VarcharProcessor).Load(int64(len(given)), given)
		assert.EqualError(t, err, "(varchar[size=5]) payload bytes are not valid UTF-8")
		assert.Nil(t, res)
	})
}
