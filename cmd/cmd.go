package reader

import (
    "os"
	"fmt"
	"encoding/json"

    "github.com/justsushant/one2n-go-bootcamp/go-mongo-oplog-parser/parser"
)

func Read(inputFile, outputFile string) error {
    // getting file object for the input file
    inputF, err := os.Open(inputFile)
    if err != nil {
		return fmt.Errorf("error while opening file: %v", err)
    }
    defer inputF.Close()

    // getting file object for the output file
    outputF, err := os.Create(outputFile)
    if err != nil {
		return fmt.Errorf("error while opening file: %v", err)
    }
    defer outputF.Close()

    // decoding the json
    decoder := json.NewDecoder(inputF)
    for {
		var obj json.RawMessage
        if err := decoder.Decode(&obj); err != nil {
            if err.Error() == "EOF" {
                break
            }
            return fmt.Errorf("error while decoding json: %v", err)
        }

        
        m := parser.NewMongoOplogParser(string(obj))
        sqlStmt, err := m.GetEquivalentSQL()
        if err != nil {
            return fmt.Errorf("error while getting equivalent sql: %v", err)
        }

        _, err = outputF.WriteString(sqlStmt)
        if err != nil {
            fmt.Println("error while writing to file: ", err)
        }
    }
	
	return nil
}