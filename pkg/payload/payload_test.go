package payload_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/payload"
	"ktdb/pkg/sys"
)

func TestNew(t *testing.T) {
	t.Run("with nil", func(t *testing.T) {
		res, err := payload.New(nil)
		assert.NoError(t, err)
		assert.Equal(t, sys.IntByteSize, len(res))
	})
	t.Run("with 0 bytes", func(t *testing.T) {
		res, err := payload.New(make([]byte, 0))
		assert.NoError(t, err)
		assert.Equal(t, sys.IntByteSize, len(res))
	})
	t.Run("with 4 bytes", func(t *testing.T) {
		res, err := payload.New(make([]byte, 4))
		assert.NoError(t, err)
		assert.Equal(t, sys.IntByteSize+4, len(res))
	})
}

func TestSize(t *testing.T) {
	t.Run("success - no extra", func(t *testing.T) {
		expected := 0
		given := payload.Payload(IntAsBytes(expected))
		res, err := given.Size()
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("success - with extra", func(t *testing.T) {
		expected := []byte("ktsivkov")
		given := payload.Payload(IntAsBytes(len(expected)))
		given = append(given, expected...)
		res, err := given.Size()

		assert.NoError(t, err)
		assert.Equal(t, len(expected), res)
		assert.Equal(t, expected, []byte(given[sys.IntByteSize:]))
	})

	t.Run("fail - not enough", func(t *testing.T) {
		given := payload.Payload(make([]byte, 0))
		res, err := given.Size()

		assert.EqualError(t, err, "malformed payload")
		assert.Equal(t, 0, res)
	})
}

func TestRead(t *testing.T) {
	t.Run("success - no extra bytes", func(t *testing.T) {
		expectedSize := sys.IntByteSize + 4
		expectedBytes := make([]byte, 4)
		givenBytes := make([]byte, 4)
		given := payload.Payload(ConcatSlices(IntAsBytes(4), givenBytes))
		res, size, err := given.Read()
		assert.NoError(t, err)
		assert.Equal(t, expectedSize, size)
		assert.Equal(t, expectedBytes, res)
	})
	t.Run("success - not enough bytes", func(t *testing.T) {
		givenBytes := make([]byte, 4)
		given := payload.Payload(ConcatSlices(IntAsBytes(4), givenBytes))
		res, size, err := given[:len(given)-1].Read()
		assert.EqualError(t, err, "expected payload with size of 12 or more, got 11")
		assert.Equal(t, 0, size)
		assert.Nil(t, res)
	})
	t.Run("success - some extra bytes", func(t *testing.T) {
		expectedSize := sys.IntByteSize + 4
		expectedBytes := make([]byte, 4)
		givenBytes := make([]byte, 10)
		given := payload.Payload(ConcatSlices(IntAsBytes(4), givenBytes))
		res, size, err := given.Read()
		assert.NoError(t, err)
		assert.Equal(t, expectedSize, size)
		assert.Equal(t, expectedBytes, res)
	})
}
