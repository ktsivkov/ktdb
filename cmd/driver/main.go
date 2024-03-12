package main

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
)

type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	// in this function we need to establish a connection to the database
	return &conn{}, nil
}

type conn struct{}

func (c *conn) Begin() (driver.Tx, error) {
	//TODO implement me -- start a transaction
	panic("implement me")
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	// in this function we prepare a statement for execution, return a dummy
	return &stmt{}, nil
}

func (c *conn) Close() error {
	// this function closes the connection, returns an error if it fails
	return nil
}

type stmt struct{}

func (s *stmt) Close() error {
	// this function is to close the statement and return an error if it fails
	return nil
}

func (s *stmt) NumInput() int {
	// this function is to return the number of placeholders in the prepared statement
	return 0
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	// this function is to execute functions that do not produce output i.e. INSERT INTO
	return nil, errors.New("not implemented")
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	// this function is to execute a prepared statement that returns rows
	return &rows{}, nil
}

type rows struct{}

func (r *rows) Columns() []string {
	// Here you return the column names of the request
	return []string{
		"username",
	}
}

// Close implements the driver.Rows interface
func (r *rows) Close() error {
	// This should close the rows stream and return an error if it fails
	return nil
}

// Next implements the driver.Rows interface
func (r *rows) Next(dest []driver.Value) error {
	// This function should copy the rows values into the dest (use copy()) for rows.Scan to work. return nil if another row exists, and io.EOF if it's done
	fmt.Printf("Next called on %+v\n", dest)
	copy(dest, []driver.Value{"asd"})
	return nil
	return io.EOF
}

// This variable can be replaced with -ldflags like below:
// go build "-ldflags=-X github.com/ktsivkov/ktdb-driver.driverName=ktdb"
var driverName = "ktdb"

func init() {
	sql.Register(driverName, &Driver{})
}

func main() {
	db, err := sql.Open("ktdb", "localhost:2610")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	rows, err := db.Query("SELECT PING()")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	for rows.Next() {
		res := "asdsa"
		if err := rows.Scan(&res); err != nil {
			log.Fatal(err)
		}
		fmt.Println(res)
	}
}
