package parser

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

var idKey = "_id"

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
	genUuid func()string
}

func NewMongoOplogParser(query string) *MongoOplog {
	return &MongoOplog{
		rawOplog: query,
		genUuid: func() string {
			return primitive.NewObjectID().Hex()
		},
	}
}

func(s *MongoOplog) GetEquivalentSQL() (string, error) {
	// unmarshalling the raw oplog
	var obj interface{}
	err := json.Unmarshal([]byte(s.rawOplog), &obj)
	if err != nil {
		return "", err
	}

	// to handle both type, slice of json and single json
	var result []map[string]interface{}
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

	// parsing the raw oplog
	for _, r := range result {
		err := s.setOperationType(r)
		if err != nil {
			fmt.Println(err)
		}

		err = s.setTableName(r)
		if err != nil {
			fmt.Println(err)
		}

		err = s.setKeysAndValues(r)
		if err != nil {
			fmt.Println(err)
		}

		err = s.save()
		if err != nil {
			fmt.Println(err)
		}

		// if r has nested objects and has _id key, then proceed further
		nestedMap, ok := r["o"].(map[string]interface{})
		if !ok {
			continue
		}
		if _, ok := nestedMap[idKey]; !ok {
			continue
		}

		// preparing parent object key and value
		parentObjVal := nestedMap[idKey].(string)
		parentObjKey := s.tableName + "_" + idKey

		// handling nested objects separetly for create table and insert statement
		// to maintain consistency wrt testing
		for key, val := range nestedMap {
			if reflect.TypeOf(val).Kind() == reflect.Slice {
				// for create table statement
				createStmt, err := s.getForeignTableCreateStatement(val, key, parentObjKey, parentObjVal)
				if err != nil {
					fmt.Println(err)
					break
				}
				s.query = append(s.query, createStmt)
				
				// for insert statement
				insertStmt, err := s.getForeignTableInsertStatement(val, key, parentObjKey, parentObjVal)
				if err != nil {
					fmt.Println(err)
					break
				}
				s.query = append(s.query, insertStmt...)
			}
		}

		for key, val := range nestedMap {
			if reflect.TypeOf(val).Kind() == reflect.Map {
				// for create table statement
				createStmt, err := s.getForeignTableCreateStatement(val, key, parentObjKey, parentObjVal)
				if err != nil {
					fmt.Println(err)
					break
				}
				s.query = append(s.query, createStmt)

				// for insert statement
				insertStmt, err := s.getForeignTableInsertStatement(val, key, parentObjKey, parentObjVal)
				if err != nil {
					fmt.Println(err)
					break
				}
				s.query = append(s.query, insertStmt...)
			}
		}
	}

	// maybe we need to reset a lot of fields here like setMap, unsetMap, conditionMap, etc
	
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

func(s *MongoOplog) setKeysAndValues(result map[string]interface{}) error {
    nestedMap, ok := result["o"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("error: o key not found in the oplog: failed to set keys and values")
	}

	// parsing the schema only once
	// if any new key is found, alter table statement is added atm of handling insert operation
	if !s.isSchemaParsed {
		s.tableCols = make(map[string]string)
		for key, val := range nestedMap {
			// skip if value is map or slice
			if reflect.TypeOf(val).Kind() == reflect.Map || reflect.TypeOf(val).Kind() == reflect.Slice {
				continue
			}

			s.tableCols[key] = s.getTableColType(key, val)
		}
		s.isSchemaParsed = true
	}

   	if s.op == "i" {	// on insert operation
		s.keys = make([]string, 0, len(nestedMap))
		s.vals = make([]string, 0, len(nestedMap))
		
		// extracts the insert key and values
		for key, val := range nestedMap {
			// skip if value is map or slice
			if reflect.TypeOf(val).Kind() == reflect.Map || reflect.TypeOf(val).Kind() == reflect.Slice {
				continue
			}

			// if key is not in table schema, add alter table statement
			if _, ok := s.tableCols[key]; !ok {
				s.tableCols[key] = s.getTableColType(key, val)
				s.query = append(s.query, s.getAlterTableStatement(key, s.tableCols[key]))
			}

			// adding key and value for query generation
			s.keys = append(s.keys, key)
			s.vals = append(s.vals, s.convertValueToString(val))
		}
	} else if s.op == "u" {		// on update operation
		nestedMap, ok = result["o"].(map[string]interface{})["diff"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("error: diff key not found in the oplog: failed to set keys and values")
		}

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
	return nil
}

func(s *MongoOplog) save() error {
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
			return fmt.Errorf("error: keys and values length mismatch while inserting")
		}

		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);", s.dbName, s.tableName, strings.Join(s.keys, ", "), strings.Join(s.vals, ", "))
		s.query = append(s.query, insertQuery)
	} else if s.op == "u" {
		updateClause := s.getUpdateClause()
		if updateClause == "" {
			return fmt.Errorf("error: update clause not found while updating")
		}

		conditionClause := s.getConditionClause()
		if conditionClause == "" {
			return fmt.Errorf("error: condition clause not found while updating")
		}

		s.query = append(s.query, fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s;", s.dbName, s.tableName, updateClause, conditionClause))
	} else if s.op == "d" {
		conditionClause := s.getConditionClause()
		if conditionClause == "" {
			return fmt.Errorf("error: condition clause not found while deleting")
		}

		s.query = append(s.query, fmt.Sprintf("DELETE FROM %s.%s WHERE %s;", s.dbName, s.tableName, conditionClause))
	}

	return nil
}

func(s *MongoOplog) getForeignTableCreateStatement(data interface{}, fTableName, parentObjKey, parentObjVal string) (string, error) {
	var tableCols = make(map[string]string)

	// saving two id columns first
	tableCols[idKey] = s.getTableColType(idKey, s.convertValueToString(s.genUuid()))
	tableCols[parentObjKey] = s.getTableColType(parentObjKey, s.convertValueToString(parentObjVal))

	// if data is slice
	if reflect.TypeOf(data).Kind() == reflect.Slice {
		for key, val := range data.([]interface{})[0].(map[string]interface{}) {
			tableCols[key] = s.getTableColType(key, val.(string))
		}
	}

	// if data is map
	if reflect.TypeOf(data).Kind() == reflect.Map {
		for key, val := range data.(map[string]interface{}) {
			tableCols[key] = s.getTableColType(key, val.(string))
		}
	}

	cols := s.getCreateTableValues(tableCols)
	slices.Sort(cols)	// sorting table columns to maintain consistency wrt testing

	if len(cols) == 0 {
		return "", fmt.Errorf("no columns to create %s table", fTableName)
	}

	createTable := fmt.Sprintf("CREATE TABLE %s.%s_%s (%s);", s.dbName, s.tableName, fTableName, strings.Join(cols, ", "))
	return createTable, nil
}

func(s *MongoOplog) getForeignTableInsertStatement(data interface{}, fTableName, parentObjKey, parentObjVal string) ([]string, error) {
	queries := []string{}
	keysArr := []string{idKey, parentObjKey}
	valsArr := []string{s.convertValueToString(s.genUuid()), s.convertValueToString(parentObjVal)}

	// if data is slice
	if reflect.TypeOf(data).Kind() == reflect.Slice {
		for _, v := range data.([]interface{}) {
			qs := s.craftForeignTableInsertStatement(v.(map[string]interface{}), fTableName, keysArr, valsArr)
			queries = append(queries, qs...)
		}
	}

	// if data is map
	if reflect.TypeOf(data).Kind() == reflect.Map {
		qs := s.craftForeignTableInsertStatement(data.(map[string]interface{}), fTableName, keysArr, valsArr)
		queries = append(queries, qs...)
	}

	return queries, nil
}

// crafts insert statements according to data
func(s *MongoOplog) craftForeignTableInsertStatement(data map[string]interface{}, fTableName string, keysArr, valsArr []string) []string {
	queries := []string{}
	for k, v := range data {
		keysArr = append(keysArr, k)
		valsArr = append(valsArr, s.convertValueToString(v))
	}

	queries = append(queries, fmt.Sprintf("INSERT INTO %s.%s_%s (%s) VALUES (%s);", s.dbName, s.tableName, fTableName, strings.Join(keysArr, ", "), strings.Join(valsArr, ", ")))
	return queries
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