package parser

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type MongoOplog struct {
	rawOplog string
	op string
	dbName string
	tableName string
	keys []string
	vals []string
	valType []string
	setMap map[string]interface{}
	unsetMap map[string]interface{}
	conditionMap map[string]interface{}
	query []string
	isSchemaCreated bool
}

func NewMongoOplogParser(query string) *MongoOplog {
	return &MongoOplog{rawOplog: query}
}

func(s *MongoOplog) String() string {
	return strings.Join(s.query, "")
}

func(s *MongoOplog) Parse() (string, error) {
	var result []map[string]interface{}
	var obj interface{}

	err := json.Unmarshal([]byte(s.rawOplog), &obj)
	if err != nil {
		return "", err
	}

	// to handle both, slice of json and single json
	switch o := obj.(type) {
	case []interface{}:
		for _, item := range o {
            if obj, ok := item.(map[string]interface{}); ok {
                result = append(result, obj)
            }
        }
	case map[string]interface{}:
		result = append(result, o)
	}

	for _, r := range result {
		s.setOperationType(r)
		s.setTableName(r)
		s.setKeysAndValues(r)
		s.save()
	}
	
	return s.String(), nil
}

func(s *MongoOplog) save() {
	if s.op == "i" {
		if !s.isSchemaCreated{
			// sorting table columns to maintain consistency wrt testing
			slices.Sort(s.valType)

			createSchema := fmt.Sprintf("CREATE SCHEMA %s;", s.dbName)
			createTable := fmt.Sprintf("CREATE TABLE %s.%s (%s);", s.dbName, s.tableName, strings.Join(s.valType, ", "))

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

func(s *MongoOplog) setOperationType(result map[string]interface{}) {
	if result["op"] == "i" || result["op"] == "u" || result["op"] == "d" {
		s.op = result["op"].(string)
	}
}

func(s *MongoOplog) setTableName(result map[string]interface{}) {
    if result["ns"] != nil {
        s.dbName = strings.Split(result["ns"].(string), ".")[0]
		s.tableName = strings.Split(result["ns"].(string), ".")[1]
    }
}

func(s *MongoOplog) setKeysAndValues(result map[string]interface{}) {
    nestedMap := result["o"].(map[string]interface{})
   	if s.op == "i" {
		s.keys = make([]string, 0, len(nestedMap))
		s.vals = make([]string, 0, len(nestedMap))
		s.valType = make([]string, 0, len(nestedMap))
		
		// extracts the insert key and values
		for key, val := range nestedMap {
			s.keys = append(s.keys, key)
			s.vals = append(s.vals, s.convertValueToString(val))
			s.valType = append(s.valType, s.createTableColumn(key, val))
		}
	} else if s.op == "u" {
		nestedMap = result["o"].(map[string]interface{})["diff"].(map[string]interface{})

		// extracts the update set key and value
		if nestedMap["u"] != nil {
			s.setMap = make(map[string]interface{})
			for key, val := range nestedMap["u"].(map[string]interface{}) {
				s.setMap[key] = val
			}
		}

		// extracts the update unset key and value
		if nestedMap["d"] != nil {
			s.unsetMap = make(map[string]interface{})
			for key, val := range nestedMap["d"].(map[string]interface{}) {
				s.unsetMap[key] = val
			}
		}

		// extracts the update condition
		if result["o2"] != nil {
			s.conditionMap = make(map[string]interface{})
			for key, val := range result["o2"].(map[string]interface{}) {
				s.conditionMap[key] = val
			}
		}
	} else if s.op == "d" {
		nestedMap = result["o"].(map[string]interface{})
		s.conditionMap = make(map[string]interface{})
		for key, val := range nestedMap {
			s.conditionMap[key] = val
		}
	}
}

func(s *MongoOplog) getConditionClause() string {
	var conditionClause string
	for key, val := range s.conditionMap {
		conditionClause = fmt.Sprintf("%v = %v", key, s.convertValueToString(val))
	}

	return conditionClause
}

func(s *MongoOplog) getUpdateClause() string {
	var updateClause string

	for key, val := range s.setMap {
		updateClause = fmt.Sprintf("%v = %v", key, s.convertValueToString(val))
	}

	for key := range s.unsetMap {
		updateClause = fmt.Sprintf("%s = NULL", key)
	}

	return updateClause
}

func(s *MongoOplog) createTableColumn(key string, val interface{}) string {
	switch val.(type) {
	case string:
		// assuming _id is primary key
		if key == "_id" {
			return key + " VARCHAR(255) PRIMARY KEY"
		}
		return key + " VARCHAR(255)"
	case float64:
		return key + " FLOAT"
	case bool:
		return key + " BOOLEAN"
	default:
		return ""
	}
}

func(s *MongoOplog) convertValueToString(val interface{}) string {
	// json unmarshalling converts all numbers to float64
    switch v := val.(type) {
    case string:
        return "'" + v + "'"
    case float64:
		// number is int
        if v == float64(int(v)) {
            return strconv.Itoa(int(v))
        }
		// number is float
        return strconv.FormatFloat(v, 'f', -1, 64)
    case bool:
        return strconv.FormatBool(v)
    default:
        return ""
    }
}