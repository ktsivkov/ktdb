package main

import (
	"fmt"
	"log"

	"ktdb/pkg/engine/parser"
	"ktdb/pkg/engine/parser/tokenizer"
	"ktdb/pkg/engine/sql"
)

func main() {
	p, err := parser.NewSqlParser(tokenizer.NewSqlTokenizer(), []parser.StatementParser{
		sql.NewSelectParser(),
	})
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := p.Parse("SELECT username FROM users WHERE id > 0 and name = 'ktsivkov' and age >= 18")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(stmt.Json())
}
