package column_types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/internal/column_types"
	"ktdb/pkg/payload"
)

func TestStr_Marshal(t *testing.T) {
	myStr := column_types.Str("ktsivkov")
	expected, _ := payload.New([]byte("ktsivkov"))

	res, err := myStr.Marshal()
	assert.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestStr_Unmarshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expected := column_types.Str("ktsivkov")
		given, _ := payload.New([]byte("ktsivkov"))

		res, err := new(column_types.Str).Unmarshal(given)
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("fail - bad payload", func(t *testing.T) {
		given, _ := payload.New([]byte{0xC3, 0x28, 0x3F, 0xE2, 0x82}) // An invalid sequence of bytes for a string

		res, err := new(column_types.Str).Unmarshal(given)
		assert.EqualError(t, err, "(string) payload bytes not valid UTF-8")
		assert.Nil(t, res)
	})
}
