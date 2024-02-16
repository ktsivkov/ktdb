package grid_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"ktdb/internal/grid"
	"ktdb/pkg/data"
)

func TestNewRowSchema(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		res, err := grid.NewRowSchema([]*grid.ColumnSchema{
			{
				Name:       "test-col-1",
				ColumnSize: 1,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			},
			{
				Name:       "test-col-2",
				ColumnSize: 1,
				Nullable:   false,
				Default:    nil,
				Type:       reflect.TypeOf(TestByteColMock(0x00)),
			},
		})
		assert.NoError(t, err)
		assert.IsType(t, &grid.RowSchema{}, res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("duplicate column", func(t *testing.T) {
			res, err := grid.NewRowSchema([]*grid.ColumnSchema{
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       reflect.TypeOf(TestByteColMock(0x00)),
				},
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       reflect.TypeOf(TestByteColMock(0x00)),
				},
			})
			assert.EqualError(t, err, "(row=[column_position=1, column_name=test-col]) already exists")
			assert.Nil(t, res)
		})
		t.Run("not defined", func(t *testing.T) {
			res, err := grid.NewRowSchema([]*grid.ColumnSchema{
				nil,
			})
			assert.EqualError(t, err, "(row=[column_position=0]) is not defined")
			assert.Nil(t, res)
		})
		t.Run("missing a name", func(t *testing.T) {
			res, err := grid.NewRowSchema([]*grid.ColumnSchema{
				{
					Name:       "",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       reflect.TypeOf(TestByteColMock(0x00)),
				},
			})
			assert.EqualError(t, err, "(row=[column_position=0]) is missing a name")
			assert.Nil(t, res)
		})
		t.Run("missing a type", func(t *testing.T) {
			res, err := grid.NewRowSchema([]*grid.ColumnSchema{
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    nil,
					Type:       nil,
				},
			})
			assert.EqualError(t, err, "(row=[column_position=0, column_name=test-col]) is missing a type")
			assert.Nil(t, res)
		})
		t.Run("default value type mismatch", func(t *testing.T) {
			type differentType struct {
				TestByteColMock
			}
			res, err := grid.NewRowSchema([]*grid.ColumnSchema{
				{
					Name:       "test-col",
					ColumnSize: 1,
					Nullable:   false,
					Default:    differentType{TestByteColMock(0x00)},
					Type:       reflect.TypeOf(TestByteColMock(0x00)),
				},
			})
			assert.EqualError(t, err, "(row=[column_position=0, column_name=test-col]) default value validation failed: (column=[name=test-col]) given type [name=col_mock, type=grid_test.differentType] doesn't match required type [name=col_mock, type=grid_test.TestByteColMock]")
			assert.Nil(t, res)
		})
	})
}

func getRowSchema() *grid.RowSchema {
	res, _ := grid.NewRowSchema([]*grid.ColumnSchema{
		{
			Name:       "test-col-1",
			ColumnSize: 1,
			Nullable:   false,
			Default:    nil,
			Type:       reflect.TypeOf(TestByteColMock(0x00)),
		},
		{
			Name:       "test-col-2",
			ColumnSize: 1,
			Nullable:   true,
			Default:    nil,
			Type:       reflect.TypeOf(TestByteColMock(0x00)),
		},
		{
			Name:       "test-col-3",
			ColumnSize: 1,
			Nullable:   true,
			Default:    TestByteColMock(0xFF),
			Type:       reflect.TypeOf(TestByteColMock(0x00)),
		},
	})
	return res
}

func TestRowSchema_Prepare(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		expected := []data.Column{
			TestByteColMock(0xFF),
			nil,
			TestByteColMock(0xFF),
		}
		schema := getRowSchema()
		res, err := schema.Prepare(map[string]data.Column{
			"test-col-1": TestByteColMock(0xFF),
		})
		assert.NoError(t, err)
		assert.Equal(t, expected, res)
	})
	t.Run("fail", func(t *testing.T) {
		t.Run("validation fail", func(t *testing.T) {
			schema := getRowSchema()
			res, err := schema.Prepare(map[string]data.Column{})
			assert.EqualError(t, err, "validation failed: (column=[name=test-col-1]) is not nullable")
			assert.Nil(t, res)
		})
	})
}

func TestRowSchema_Row(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		schema := getRowSchema()
		res, err := schema.Row([]data.Column{
			TestByteColMock(0xFF),
			nil,
			TestByteColMock(0xFF),
		})
		assert.NoError(t, err)
		assert.Len(t, res, schema.ByteSize())
	})

	t.Run("fail", func(t *testing.T) {
		t.Run("wrong column number", func(t *testing.T) {
			schema := getRowSchema()
			res, err := schema.Row([]data.Column{})
			assert.EqualError(t, err, "expected columns [size=0], got [size=3]")
			assert.Nil(t, res)
		})
		t.Run("marshal error", func(t *testing.T) {
			schema := getRowSchema()
			res, err := schema.Row([]data.Column{
				TestByteColMock(0x00),
				nil,
				TestByteColMock(0xFF),
			})
			assert.EqualError(t, err, "could not marshal column: (column=[name=test-col-1]) marshal failed: error")
			assert.Nil(t, res)
		})
	})
}

func TestRowSchema_Columns(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		schema := getRowSchema()
		res, err := schema.Columns(grid.Row{
			// NullFlag, Value
			0xFF, 0xFF,
			0x00, 0x00,
			0xFF, 0xFF,
		})
		assert.NoError(t, err)
		assert.Equal(t, []data.Column{
			TestByteColMock(0xFF),
			nil,
			TestByteColMock(0xFF),
		}, res)
	})

	t.Run("fail", func(t *testing.T) {
		t.Run("wrong size of row", func(t *testing.T) {
			schema := getRowSchema()
			res, err := schema.Columns(nil)
			assert.EqualError(t, err, "expected row of size [bytes=6], got [bytes=0]")
			assert.Nil(t, res)
		})
		t.Run("unmarshal error", func(t *testing.T) {
			schema := getRowSchema()
			res, err := schema.Columns(grid.Row{
				// NullFlag, Value
				0x00, 0x00,
				0x00, 0x00,
				0xFF, 0xFF,
			})
			assert.EqualError(t, err, "failed unmarshalling column: (column=[name=test-col-1]) corrupted data, cannot assign null on a not-nullable column")
			assert.Nil(t, res)
		})
	})
}
