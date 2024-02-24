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
	//sc := data.ColumnSchema{
	//	Name:       "username",
	//	ColumnSize: 255,
	//	Nullable:   false,
	//	Default:    column_types2.Varchar("ktsivkov"),
	//	Type:       reflect.TypeOf(column_types2.Varchar("")),
	//}
	//schemaBytes, err := sc.Bytes()
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//loaded, err := data.LoadColumnSchemaFromBytes(schemaBytes, types.Get)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//fmt.Println("schema", sc)
	//fmt.Println("schema exported bytes", schemaBytes)
	//fmt.Println("restored schema", loaded)
	//return

	schema, err := data.NewRowSchema([]*data.ColumnSchema{
		{
			Name:       "username",
			ColumnSize: 32,
			Nullable:   false,
			Default:    nil,
			Type:       reflect.TypeOf(column_types.Varchar("")),
		},
		{
			Name:       "age",
			ColumnSize: 8,
			Nullable:   false,
			Default:    nil,
			Type:       reflect.TypeOf(column_types.Int(0)),
		},
		{
			Name:       "signature",
			ColumnSize: 32,
			Nullable:   false,
			Default:    column_types.Varchar("no signature yet"),
			Type:       reflect.TypeOf(column_types.Varchar("")),
		},
		{
			Name:       "rating",
			ColumnSize: 8,
			Nullable:   true,
			Default:    nil,
			Type:       reflect.TypeOf(column_types.Int(0)),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	prepared, err := schema.Prepare(map[string]data.Column{
		"username": column_types.Varchar("ktsivkov"),
		"age":      column_types.Int(18),
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("prepared", prepared)
	res, err := schema.Row(prepared)
	if err != nil {
		log.Fatal(err)
	}
	cols, err := schema.Columns(columnProcessor, res)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cols)
}
