package column_types_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/internal/column_types"
	"ktdb/pkg/payload"
	"ktdb/pkg/sys"
)

func TestInt_Marshal(t *testing.T) {
	myInt := column_types.Int(5)
	expected := make(payload.Payload, sys.IntByteSize)
	if sys.IntByteSize == 2 {
		binary.LittleEndian.PutUint16(expected, 5)
	}
	if sys.IntByteSize == 4 {
		binary.LittleEndian.PutUint32(expected, 5)
	}
	if sys.IntByteSize == 8 {
		binary.LittleEndian.PutUint64(expected, 5)
	}
	expected, _ = payload.New(expected)

	res, err := myInt.Marshal()
	assert.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestInt_Unmarshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expected := column_types.Int(5)
		given := make([]byte, sys.IntByteSize)
		if sys.IntByteSize == 2 {
			binary.LittleEndian.PutUint16(given, 5)
		}
		if sys.IntByteSize == 4 {
			binary.LittleEndian.PutUint32(given, 5)
		}
		if sys.IntByteSize == 8 {
			binary.LittleEndian.PutUint64(given, 5)
		}
		given, _ = payload.New(given)

		res, err := new(column_types.Int).Unmarshal(given)
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("fail - bad payload", func(t *testing.T) {
		given, _ := payload.New(make([]byte, sys.IntByteSize-1)) // Subtract 1 from sys.IntByteSize to ensure given bytes are not enough to produce an int

		res, err := new(column_types.Int).Unmarshal(given)
		assert.EqualError(t, err, "(int) payload byte count mismatch")
		assert.Nil(t, res)
	})
}
