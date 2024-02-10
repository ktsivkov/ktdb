package main

import (
	"fmt"
	"log"
	"reflect"

	"ktdb/internal/column_types"
	"ktdb/internal/table"
	"ktdb/pkg/data"
)

func main() {
	rowSchema := table.RowSchema{
		{
			Name: "username",
			Type: reflect.TypeOf(column_types.Str("")),
			Rules: []table.ColumnRuleFunc{
				table.WithNotNullValueRule(),
			},
		},
		{
			Name:    "age",
			Type:    reflect.TypeOf(column_types.Int(0)),
			Default: column_types.Int(18),
			Rules: []table.ColumnRuleFunc{
				table.WithNotNullValueRule(),
			},
		},
		{
			Name:  "country",
			Type:  reflect.TypeOf(column_types.Str("")),
			Rules: []table.ColumnRuleFunc{},
		},
	}

	row, err := rowSchema.Row(map[string]data.Column{"username": column_types.Str("ktsivkov"), "country": nil})
	if err != nil {
		log.Fatal(err)
	}

	tbl := table.Table{
		Name:      "users",
		RowSchema: rowSchema,
	}
	tbl.Append(row)

	bytes, err := tbl.Bytes()
	if err != nil {
		log.Fatal(err)
	}

	tbl2 := table.Table{
		Name:      "users",
		RowSchema: rowSchema,
	}

	fmt.Println(tbl2.Load(bytes))

	col, err := tbl2.Rows[0].Get("country")
	fmt.Println(col == nil)
}
