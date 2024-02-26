package sys_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/sys"
)

func TestNew(t *testing.T) {
	t.Run("with nil", func(t *testing.T) {
		res := sys.New(nil)
		assert.Equal(t, sys.IntByteSize, int64(len(res)))
	})
	t.Run("with 0 bytes", func(t *testing.T) {
		res := sys.New(make([]byte, 0))
		assert.Equal(t, sys.IntByteSize, int64(len(res)))
	})
	t.Run("with 4 bytes", func(t *testing.T) {
		res := sys.New(make([]byte, 4))
		assert.Equal(t, sys.IntByteSize+4, int64(len(res)))
	})
}

func TestSize(t *testing.T) {
	t.Run("success - no extra", func(t *testing.T) {
		expected := int64(0)
		given := sys.Int64AsBytes(expected)
		res, err := sys.Size(given)
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("success - with extra", func(t *testing.T) {
		expected := []byte("ktsivkov")
		given := sys.Int64AsBytes(int64(len(expected)))
		given = append(given, expected...)
		res, err := sys.Size(given)

		assert.NoError(t, err)
		assert.Equal(t, int64(len(expected)), res)
		assert.Equal(t, expected, given[sys.IntByteSize:])
	})

	t.Run("fail - not enough", func(t *testing.T) {
		given := make([]byte, 0)
		res, err := sys.Size(given)

		assert.EqualError(t, err, "payload has no size defined")
		assert.Equal(t, int64(0), res)
	})
}

func TestRead(t *testing.T) {
	t.Run("success - no extra bytes", func(t *testing.T) {
		expectedSize := sys.IntByteSize + 4
		expectedBytes := make([]byte, 4)
		givenBytes := make([]byte, 10)
		given := sys.ConcatSlices(sys.Int64AsBytes(4), givenBytes)
		res, size, err := sys.Read(given)
		assert.NoError(t, err)
		assert.Equal(t, expectedSize, size)
		assert.Equal(t, expectedBytes, res)
	})
	t.Run("success - some extra bytes", func(t *testing.T) {
		expectedSize := sys.IntByteSize + 4
		expectedBytes := make([]byte, 4)
		givenBytes := make([]byte, 4)
		given := sys.ConcatSlices(sys.Int64AsBytes(4), givenBytes)
		res, size, err := sys.Read(given)
		assert.NoError(t, err)
		assert.Equal(t, expectedSize, size)
		assert.Equal(t, expectedBytes, res)
	})
}
