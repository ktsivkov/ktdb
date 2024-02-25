package main

import (
	"fmt"
	"log"

	"ktdb/pkg/column_types"
	"ktdb/pkg/data"
	"ktdb/pkg/engine"
)

func main() {
	varcharProcessor := &column_types.VarcharProcessor{}
	intProcessor := &column_types.IntProcessor{}
	columnProcessor, err := engine.NewColumnProcessor([]engine.ColumnTypeProcessor{
		varcharProcessor,
		intProcessor,
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
			Type:       varcharProcessor.Type(),
		},
		{
			Name:       "age",
			ColumnSize: 8,
			Nullable:   false,
			Default:    nil,
			Type:       intProcessor.Type(),
		},
		{
			Name:       "signature",
			ColumnSize: 32,
			Nullable:   false,
			Default:    column_types.Varchar("no signature yet"),
			Type:       varcharProcessor.Type(),
		},
		{
			Name:       "rating",
			ColumnSize: 8,
			Nullable:   true,
			Default:    nil,
			Type:       intProcessor.Type(),
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
