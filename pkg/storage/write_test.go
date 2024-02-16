package storage_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/pkg/storage"
)

func TestCreateOrOverride(t *testing.T) {
	t.Run("create file", func(t *testing.T) {
		given := []byte("hello world")
		filename := "test-file.txt"
		err := storage.CreateOrOverride(filename, given)
		assert.NoError(t, err)
		actual, err := os.ReadFile(filename)
		assert.NoError(t, err, "reading test created file")
		assert.Equal(t, given, actual)
		err = os.Remove(filename)
		assert.NoError(t, err, "removing the test created file")
	})
	t.Run("replace file", func(t *testing.T) {
		given := []byte("hello world")
		filename := "test-file.txt"
		file, err := os.Create(filename)
		assert.NoError(t, err, "could not create test tile")

		_, err = file.Write([]byte("1234567890"))
		assert.NoError(t, err, "could not write to the test created file")
		assert.NoError(t, file.Close(), "could not close test file")

		assert.NoError(t, storage.CreateOrOverride(filename, given))
		actual, err := os.ReadFile(filename)
		assert.NoError(t, err, "reading test created file")
		assert.Equal(t, given, actual)
		err = os.Remove(filename)
		assert.NoError(t, err, "removing the test created file")
	})
}

func TestAppend(t *testing.T) {
	contents := []byte("01234567890")
	given := []byte("hello world")
	expected := append(contents, given...)
	filename := "test-file.txt"
	file, err := os.Create(filename)
	assert.NoError(t, err, "could not create test tile")
	_, err = file.Write(contents)
	assert.NoError(t, err, "could not write to the test created file")
	assert.NoError(t, file.Close(), "could not close test file")

	assert.NoError(t, storage.Append(filename, given))

	actual, err := os.ReadFile(filename)
	assert.NoError(t, err, "reading test created file")
	assert.Equal(t, expected, actual)
	err = os.Remove(filename)
	assert.NoError(t, err, "removing the test created file")
}

func TestOffset(t *testing.T) {
	contents := []byte("01234567890")
	given := []byte("-hello world-")
	expected := []byte("0123456789-hello world-")
	filename := "test-file.txt"
	file, err := os.Create(filename)
	assert.NoError(t, err, "could not create test tile")
	_, err = file.Write(contents)
	assert.NoError(t, err, "could not write to the test created file")
	assert.NoError(t, file.Close(), "could not close test file")

	assert.NoError(t, storage.Offset(filename, int64(len(contents)-1), given))

	actual, err := os.ReadFile(filename)
	assert.NoError(t, err, "reading test created file")
	assert.Equal(t, expected, actual)
	err = os.Remove(filename)
	assert.NoError(t, err, "removing the test created file")
}

func TestReplace(t *testing.T) {
	firstPart := []byte("i said ")
	toBeReplaced := []byte("SOMETHING")
	secondPart := []byte(", and I mean it")
	contents := append(firstPart, append(toBeReplaced, secondPart...)...)
	filename := "test-file.txt"
	file, err := os.Create(filename)
	assert.NoError(t, err, "could not create test tile")
	_, err = file.Write(contents)
	assert.NoError(t, err, "could not write to the test created file")
	assert.NoError(t, file.Close(), "could not close test file")

	given := []byte("-hello world-")
	expected := append(firstPart, append(given, secondPart...)...)

	assert.NoError(t, storage.Replace(filename, int64(len(firstPart)), int64(len(firstPart)+len(toBeReplaced)), given))

	actual, err := os.ReadFile(filename)
	assert.NoError(t, err, "reading test created file")
	assert.Equal(t, expected, actual)
	err = os.Remove(filename)
	assert.NoError(t, err, "removing the test created file")
}
