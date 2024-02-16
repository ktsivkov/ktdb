package column_types_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/internal/column_types"
	"ktdb/pkg/sys"
)

func TestInt_Marshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		architectures := map[string]int{
			"16 bit": 2,
			"32 bit": 4,
			"64 bit": 8,
		} // 16, 32, 64 bit into bytes
		for testName, architecture := range architectures {
			t.Run(testName, func(t *testing.T) {
				if architecture > sys.IntByteSize {
					t.Skip(fmt.Sprintf("unsupported architecture int size of %d bytes", architecture))
				}
				myInt := column_types.Int(5)
				expected := make([]byte, architecture)
				if architecture == 2 {
					binary.LittleEndian.PutUint16(expected, 5)
				}
				if architecture == 4 {
					binary.LittleEndian.PutUint32(expected, 5)
				}
				if architecture == 8 {
					binary.LittleEndian.PutUint64(expected, 5)
				}

				res, err := myInt.Marshal(architecture)
				assert.NoError(t, err)
				assert.Equal(t, expected, res)
			})
		}
	})
	t.Run("fail - unsupported size", func(t *testing.T) {
		myInt := column_types.Int(5)
		givenSize := sys.IntByteSize - 1
		res, err := myInt.Marshal(givenSize)
		assert.EqualError(t, err, fmt.Sprintf("(int[size=%d]) unsupported size", givenSize))
		assert.Nil(t, res)
	})
}

func TestInt_Unmarshal(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		architectures := map[string]int{
			"16 bit": 2,
			"32 bit": 4,
			"64 bit": 8,
		} // 16, 32, 64 bit into bytes
		for testName, architecture := range architectures {
			t.Run(testName, func(t *testing.T) {
				if architecture > sys.IntByteSize {
					t.Skip(fmt.Sprintf("unsupported architecture int size of %d bytes", architecture))
				}
				expected := column_types.Int(5)
				given := make([]byte, architecture)
				if architecture == 2 {
					binary.LittleEndian.PutUint16(given, 5)
				}
				if architecture == 4 {
					binary.LittleEndian.PutUint32(given, 5)
				}
				if architecture == 8 {
					binary.LittleEndian.PutUint64(given, 5)
				}

				res, err := new(column_types.Int).Unmarshal(architecture, given)
				assert.NoError(t, err)
				assert.Equal(t, expected, res)
			})
		}
	})

	t.Run("fail - bad payload", func(t *testing.T) {
		given := make([]byte, sys.IntByteSize-1) // Subtract 1 from sys.IntByteSize to ensure given bytes are not enough to produce an int

		res, err := new(column_types.Int).Unmarshal(sys.IntByteSize, given)
		assert.EqualError(t, err, fmt.Sprintf("(int[size=%d]) payload byte size [size=%d] exceeds allocated size", sys.IntByteSize, sys.IntByteSize-1))
		assert.Nil(t, res)
	})

	t.Run("fail - unsupported size", func(t *testing.T) {
		givenSize := sys.IntByteSize - 1
		given := make([]byte, givenSize) // Subtract 1 from sys.IntByteSize to ensure given bytes are not enough to produce an int
		res, err := new(column_types.Int).Unmarshal(givenSize, given)
		assert.EqualError(t, err, fmt.Sprintf("(int[size=%d]) unsupported size", givenSize))
		assert.Nil(t, res)
	})
}
