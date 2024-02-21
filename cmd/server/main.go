package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"

	"ktdb/pkg/column_types"
	"ktdb/pkg/data"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	_, err := data.NewTypes([]reflect.Type{
		reflect.TypeOf(column_types.Varchar("")),
		reflect.TypeOf(column_types.Int(0)),
		nil,
	})
	if err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("Failed setting up data types: %s", err))
	}

}
