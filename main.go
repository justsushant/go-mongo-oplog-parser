package main

import (
	"fmt"
	"github.com/justsushant/one2n-go-bootcamp/go-mongo-oplog-parser/parser"
)

// trying out the parser package
func main() {
	query := `[
		{
			"op": "i",
			"ns": "test.student",
			"o": {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "Selena Miller",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
			}
		},
		{
			"op": "i",
			"ns": "test.student",
			"o": {
			"_id": "14798c213f273a7ca2cf5174",
			"name": "George Smith",
			"roll_no": 21,
			"is_graduated": true,
			"date_of_birth": "2001-03-23"
			}
		}
	]`;
	oplog := parser.NewMongoOplogParser(query)
	if comm, err := oplog.Parse(); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(comm)
	}
}