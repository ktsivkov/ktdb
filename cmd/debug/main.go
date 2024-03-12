package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"ktdb/pkg/engine/storage"
	"ktdb/pkg/engine/structure"
)

func main() {
	ctx := context.Background()
	dataPath, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	systemStorage, err := storage.New(dataPath)
	if err != nil {
		log.Fatal(err)
	}

	dataStorage, err := systemStorage.NewLayer("data")
	if err != nil {
		log.Fatal(err)
	}

	systemStructure, err := structure.New(dataStorage)
	if err != nil {
		log.Fatal(err)
	}
	db1, err := systemStructure.Create(ctx, "db1")
	if err != nil {
		log.Fatal(err)
	}
	if _, err = db1.Create(ctx, "sch1"); err != nil {
		log.Fatal(err)
	}
	if _, err = db1.Create(ctx, "sch2"); err != nil {
		log.Fatal(err)
	}
	if _, err = db1.Create(ctx, "sch3"); err != nil {
		log.Fatal(err)
	}
	db2, err := systemStructure.Create(ctx, "db2")
	if err != nil {
		log.Fatal(err)
	}
	if _, err = db2.Create(ctx, "sch1"); err != nil {
		log.Fatal(err)
	}
	if _, err = db2.Create(ctx, "sch2"); err != nil {
		log.Fatal(err)
	}

	dbs, err := systemStructure.List(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, db := range dbs {
		fmt.Println(fmt.Sprintf("%s", db.Name()))
		schemas, err := db.List(ctx)
		if err != nil {
			log.Fatal(err)
		}
		for _, schema := range schemas {
			fmt.Println(fmt.Sprintf("\t%s", schema.Name()))
			tables, err := schema.List(ctx)
			if err != nil {
				log.Fatal(err)
			}
			for _, table := range tables {
				fmt.Println(fmt.Sprintf("\t\t%s", table.Name()))
			}
		}
	}
}

//func main() {
//	reader := storage.NewReader()
//	writer := storage.NewWriter()
//	tableProcessor := table.NewProcessor(reader, writer)
//	varcharProcessor := &column_types.VarcharProcessor{}
//	intProcessor := &column_types.IntProcessor{}
//	columnProcessor, err := column.NewProcessor([]column.TypeProcessor{
//		varcharProcessor,
//		intProcessor,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	rowProcessor, err := row.NewProcessor(columnProcessor)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Logic
//	usersSchema, err := getUserSchema(rowProcessor, varcharProcessor, intProcessor)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	usersTable, err := tableProcessor.New("users", usersSchema)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if err := appendUsersRow(rowProcessor, usersSchema, usersTable); err != nil {
//		log.Fatal(err)
//	}
//
//	appendedColumns, err := getFirstUsersRow(columnProcessor, usersSchema, usersTable)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println("appended", appendedColumns)
//
//	if err := updateFirstUsersRow(rowProcessor, usersSchema, usersTable); err != nil {
//		log.Fatal(err)
//	}
//
//	updatedColumns, err := getFirstUsersRow(columnProcessor, usersSchema, usersTable)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println("updated", updatedColumns)
//}
//
//func getFirstUsersRow(columnProcessor column.Processor, usersSchema *row.Schema, usersTable table.Table) ([]column.Column, error) {
//	readRow, err := usersTable.Row(1)
//	if err != nil {
//		return nil, err
//	}
//	return usersSchema.Columns(columnProcessor, readRow)
//}
//
//func updateFirstUsersRow(rowProcessor row.Processor, usersSchema *row.Schema, usersTable table.Table) error {
//	prepared, err := rowProcessor.Prepare(usersSchema, map[string]column.Column{
//		"username": column_types.Varchar("updated"),
//		"age":      column_types.Int(18),
//	})
//	if err != nil {
//		return err
//	}
//
//	newRow, err := usersSchema.Row(prepared)
//	if err != nil {
//		return err
//	}
//
//	return usersTable.Set(1, newRow)
//}
//
//func appendUsersRow(rowProcessor row.Processor, usersSchema *row.Schema, usersTable table.Table) error {
//	prepared, err := rowProcessor.Prepare(usersSchema, map[string]column.Column{
//		"username": column_types.Varchar("ktsivkov"),
//		"age":      column_types.Int(18),
//	})
//	if err != nil {
//		return err
//	}
//
//	newRow, err := usersSchema.Row(prepared)
//	if err != nil {
//		return err
//	}
//
//	return usersTable.Append(newRow)
//}
//
//func getUserSchema(rowProcessor row.Processor, varcharProcessor *column_types.VarcharProcessor, intProcessor *column_types.IntProcessor) (*row.Schema, error) {
//	return rowProcessor.New([]*column.Schema{
//		{
//			Name:     "username",
//			Size:     32,
//			Nullable: false,
//			Default:  nil,
//			Type:     varcharProcessor.Type(),
//		},
//		{
//			Name:     "age",
//			Size:     8,
//			Nullable: false,
//			Default:  nil,
//			Type:     intProcessor.Type(),
//		},
//		{
//			Name:     "signature",
//			Size:     32,
//			Nullable: false,
//			Default:  func() []byte { res, _ := column_types.Varchar("no signature yet").Bytes(32); return res }(),
//			Type:     varcharProcessor.Type(),
//		},
//		{
//			Name:     "rating",
//			Size:     8,
//			Nullable: true,
//			Default:  nil,
//			Type:     intProcessor.Type(),
//		},
//	})
//}
