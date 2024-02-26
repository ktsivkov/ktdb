package main

import (
	"fmt"
	"log"

	"ktdb/pkg/column_types"
	"ktdb/pkg/engine/grid/column"
	"ktdb/pkg/engine/grid/row"
	"ktdb/pkg/engine/table"
	"ktdb/pkg/storage"
)

func main() {
	reader := storage.NewReader()
	writer := storage.NewWriter()
	tableProcessor := table.NewProcessor(reader, writer)
	varcharProcessor := &column_types.VarcharProcessor{}
	intProcessor := &column_types.IntProcessor{}
	columnProcessor, err := column.NewProcessor([]column.TypeProcessor{
		varcharProcessor,
		intProcessor,
	})
	if err != nil {
		log.Fatal(err)
	}
	rowProcessor, err := row.NewProcessor(columnProcessor)
	if err != nil {
		log.Fatal(err)
	}

	// Logic
	usersSchema, err := getUserSchema(rowProcessor, varcharProcessor, intProcessor)
	if err != nil {
		log.Fatal(err)
	}

	usersTable, err := tableProcessor.New("users", usersSchema)
	if err != nil {
		log.Fatal(err)
	}

	if err := appendUsersRow(rowProcessor, usersSchema, usersTable); err != nil {
		log.Fatal(err)
	}

	appendedColumns, err := getFirstUsersRow(columnProcessor, usersSchema, usersTable)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("appended", appendedColumns)

	if err := updateFirstUsersRow(rowProcessor, usersSchema, usersTable); err != nil {
		log.Fatal(err)
	}

	updatedColumns, err := getFirstUsersRow(columnProcessor, usersSchema, usersTable)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("updated", updatedColumns)
}

func getFirstUsersRow(columnProcessor column.Processor, usersSchema *row.Schema, usersTable table.Table) ([]column.Column, error) {
	readRow, err := usersTable.Row(1)
	if err != nil {
		return nil, err
	}
	return usersSchema.Columns(columnProcessor, readRow)
}

func updateFirstUsersRow(rowProcessor row.Processor, usersSchema *row.Schema, usersTable table.Table) error {
	prepared, err := rowProcessor.Prepare(usersSchema, map[string]column.Column{
		"username": column_types.Varchar("updated"),
		"age":      column_types.Int(18),
	})
	if err != nil {
		return err
	}

	newRow, err := usersSchema.Row(prepared)
	if err != nil {
		return err
	}

	return usersTable.Set(1, newRow)
}

func appendUsersRow(rowProcessor row.Processor, usersSchema *row.Schema, usersTable table.Table) error {
	prepared, err := rowProcessor.Prepare(usersSchema, map[string]column.Column{
		"username": column_types.Varchar("ktsivkov"),
		"age":      column_types.Int(18),
	})
	if err != nil {
		return err
	}

	newRow, err := usersSchema.Row(prepared)
	if err != nil {
		return err
	}

	return usersTable.Append(newRow)
}

func getUserSchema(rowProcessor row.Processor, varcharProcessor *column_types.VarcharProcessor, intProcessor *column_types.IntProcessor) (*row.Schema, error) {
	return rowProcessor.New([]*column.Schema{
		{
			Name:     "username",
			Size:     32,
			Nullable: false,
			Default:  nil,
			Type:     varcharProcessor.Type(),
		},
		{
			Name:     "age",
			Size:     8,
			Nullable: false,
			Default:  nil,
			Type:     intProcessor.Type(),
		},
		{
			Name:     "signature",
			Size:     32,
			Nullable: false,
			Default:  func() []byte { res, _ := column_types.Varchar("no signature yet").Bytes(32); return res }(),
			Type:     varcharProcessor.Type(),
		},
		{
			Name:     "rating",
			Size:     8,
			Nullable: true,
			Default:  nil,
			Type:     intProcessor.Type(),
		},
	})
}
