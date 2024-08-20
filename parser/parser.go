package parser

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

type MongoOplog struct {
	rawOplog string
	op string
	dbName string
	tableName string
	tableCols map[string]string			// holds the table columns schema
	keys []string						// keys for insert operation
	vals []string						// values for insert operation
	setMap map[string]string			// key-val for update set operation
	unsetMap map[string]string			// key-val for update unset operation
	conditionMap map[string]string		// key-val for condition clause
	query []string						// holds the final sql query
	isSchemaCreated bool
	isSchemaParsed bool
}

func NewMongoOplogParser(query string) *MongoOplog {
	return &MongoOplog{rawOplog: query}
}

func(s *MongoOplog) GetEquivalentSQL() (string, error) {
	var result []map[string]interface{}
	var obj interface{}

	err := json.Unmarshal([]byte(s.rawOplog), &obj)
	if err != nil {
		return "", err
	}

	// to handle both type, slice of json and single json
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Slice:
		for _, item := range obj.([]interface{}) {
            if obj, ok := item.(map[string]interface{}); ok {
                result = append(result, obj)
            }
        }
	case reflect.Map:
		result = append(result, obj.(map[string]interface{}))
	}

	for _, r := range result {
		err := s.setOperationType(r)
		if err != nil {
			fmt.Println(err)
		}

		err = s.setTableName(r)
		if err != nil {
			fmt.Println(err)
		}

		s.setKeysAndValues(r)
		s.save()		
	}
	
	return strings.Join(s.query, ""), nil
}

func(s *MongoOplog) setOperationType(result map[string]interface{}) error {
	if result["op"] == "i" || result["op"] == "u" || result["op"] == "d" {
		s.op = result["op"].(string)
		return nil
	}
	return fmt.Errorf("error: unsupported operation type %q", result["op"])
}

func(s *MongoOplog) setTableName(result map[string]interface{}) error {
    if result["ns"] != nil {
        s.dbName = strings.Split(result["ns"].(string), ".")[0]
		s.tableName = strings.Split(result["ns"].(string), ".")[1]
		return nil
    }
	return fmt.Errorf("error: ns key not found in the oplog: failed to set the table name")
}

func(s *MongoOplog) setKeysAndValues(result map[string]interface{}) {
    nestedMap := result["o"].(map[string]interface{})

	// parsing the schema only once
	// if any new key is found, alter table statement is added atm of handling insert operation
	if !s.isSchemaParsed {
		s.tableCols = make(map[string]string)
		for key, val := range nestedMap {
			s.tableCols[key] = s.getTableColType(key, val)
		}
		s.isSchemaParsed = true
	}

   	if s.op == "i" {	// on insert operation
		s.keys = make([]string, 0, len(nestedMap))
		s.vals = make([]string, 0, len(nestedMap))
		
		// extracts the insert key and values
		for key, val := range nestedMap {
			// adding key and value for query generation
			s.keys = append(s.keys, key)
			s.vals = append(s.vals, s.convertValueToString(val))

			// if key is not in table schema, add alter table statement
			if _, ok := s.tableCols[key]; !ok {
				s.tableCols[key] = s.getTableColType(key, val)
				s.query = append(s.query, s.getAlterTableStatement(key, s.tableCols[key]))
			}
		}
	} else if s.op == "u" {		// on update operation
		nestedMap = result["o"].(map[string]interface{})["diff"].(map[string]interface{})

		// extracts the update set key and value
		if nestedMap["u"] != nil {
			s.setMap = make(map[string]string)
			for key, val := range nestedMap["u"].(map[string]interface{}) {
				s.setMap[key] = s.convertValueToString(val)
			}
		}

		// extracts the update unset key and value
		if nestedMap["d"] != nil {
			s.unsetMap = make(map[string]string)
			for key, val := range nestedMap["d"].(map[string]interface{}) {
				s.unsetMap[key] = s.convertValueToString(val)
			}
		}

		// extracts the update condition
		if result["o2"] != nil {
			s.conditionMap = make(map[string]string)
			for key, val := range result["o2"].(map[string]interface{}) {
				s.conditionMap[key] = s.convertValueToString(val)
			}
		}
	} else if s.op == "d" {		// on delete operation
		s.conditionMap = make(map[string]string)
		for key, val := range nestedMap {
			s.conditionMap[key] = s.convertValueToString(val)
		}
	}
}

func(s *MongoOplog) save() {
	if s.op == "i" {
		if !s.isSchemaCreated{
			cols := s.getCreateTableValues(s.tableCols)

			createSchema := fmt.Sprintf("CREATE SCHEMA %s;", s.dbName)
			createTable := fmt.Sprintf("CREATE TABLE %s.%s (%s);", s.dbName, s.tableName, strings.Join(cols, ", "))

			s.query = append(s.query, createSchema)
			s.query = append(s.query, createTable)
			s.isSchemaCreated = true
		}

		if len(s.keys) != len(s.vals) {
			panic("keys and values length mismatch")
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);", s.dbName, s.tableName, strings.Join(s.keys, ", "), strings.Join(s.vals, ", "))
		s.query = append(s.query, insertQuery)
	} else if s.op == "u" {
		s.query = append(s.query, fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s;", s.dbName, s.tableName, s.getUpdateClause(), s.getConditionClause()))
	} else if s.op == "d" {
		s.query = append(s.query, fmt.Sprintf("DELETE FROM %s.%s WHERE %s;", s.dbName, s.tableName, s.getConditionClause()))
	}
}


func(s *MongoOplog) getAlterTableStatement(key, val string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s ADD %s %s;", s.dbName, s.tableName, key, val)
}

// need to join with AND if multiple conditions are present
func(s *MongoOplog) getConditionClause() string {
	var conditionClause string
	for key, val := range s.conditionMap {
		conditionClause = fmt.Sprintf("%v = %v", key, val)
	}

	return conditionClause
}

// need to join with AND if multiple conditions are present
func(s *MongoOplog) getUpdateClause() string {
	var updateClause string

	for key, val := range s.setMap {
		updateClause = fmt.Sprintf("%v = %v", key, val)
	}

	// unset operation value set to NULL according to problem statement
	for key := range s.unsetMap {
		updateClause = fmt.Sprintf("%s = NULL", key)
	}

	return updateClause
}

func(s *MongoOplog) getCreateTableValues(tableCols map[string]string) []string {
	var tableColumns []string
	for key, val := range tableCols {
		tableColumns = append(tableColumns, fmt.Sprintf("%v %v", key, val))
	}

	// sorting table columns to maintain consistency wrt testing
	slices.Sort(tableColumns)

	return tableColumns
}

func(s *MongoOplog) getTableColType(key string, val interface{}) string {
	switch reflect.TypeOf(val).Kind() {
	case reflect.String:
		if key == "_id" {		// assuming _id is primary key
			return " VARCHAR(255) PRIMARY KEY"
		}
		return " VARCHAR(255)"
	case reflect.Float64:
		return " FLOAT"
	case reflect.Bool:
		return " BOOLEAN"
	default:
		return ""
	}
}

func(s *MongoOplog) convertValueToString(val interface{}) string {
	// json unmarshalling converts all numbers to float64
    switch reflect.TypeOf(val).Kind() {
    case reflect.String:
        return "'" + val.(string) + "'"
	case reflect.Int:
			return strconv.Itoa(int(val.(int)))
    case reflect.Float64:
        return strconv.FormatFloat(val.(float64), 'f', -1, 64)
    case reflect.Bool:
        return strconv.FormatBool(val.(bool))
    default:
        return ""
    }
}