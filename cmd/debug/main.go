package main

import (
	"fmt"
	"log"
	"reflect"

	"ktdb/pkg/column_types"
	"ktdb/pkg/data"
	"ktdb/pkg/engine"
)

func main() {
	columnProcessor, err := engine.NewColumnProcessor([]reflect.Type{
		reflect.TypeOf(column_types.Varchar("")),
		reflect.TypeOf(column_types.Int(0)),
	})
	if err != nil {
		log.Fatal(err)
	}

	rowSchema, err := data.NewRowSchema([]*data.ColumnSchema{
		{
			Name:       "username",
			ColumnSize: 32,
			Nullable:   false,
			Default:    nil,
			Type:       column_types.Varchar("").TypeIdentifier(),
		},
		{
			Name:       "age",
			ColumnSize: 8,
			Nullable:   false,
			Default:    nil,
			Type:       column_types.Int(0).TypeIdentifier(),
		},
		{
			Name:       "signature",
			ColumnSize: 32,
			Nullable:   false,
			Default:    column_types.Varchar("no signature yet"),
			Type:       column_types.Varchar("").TypeIdentifier(),
		},
		{
			Name:       "rating",
			ColumnSize: 8,
			Nullable:   true,
			Default:    nil,
			Type:       column_types.Int(0).TypeIdentifier(),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	prepared, err := rowSchema.Prepare(map[string]engine.Column{
		"username": column_types.Varchar("ktsivkov"),
		"age":      column_types.Int(18),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("prepared", prepared)

	res, err := rowSchema.Row(prepared)
	if err != nil {
		log.Fatal(err)
	}

	rowSchemaBytes, err := rowSchema.Bytes()
	if err != nil {
		log.Fatal(err)
	}

	restoredRowSchema, err := data.LoadRowSchemaFromBytes(columnProcessor, rowSchemaBytes)
	if err != nil {
		log.Fatal(err)
	}

	cols, err := restoredRowSchema.Columns(columnProcessor, res)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("restored", cols)
}
