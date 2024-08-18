package parser

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type SqlCommand struct {
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
}

func(s *SqlCommand) String() string {
	if s.op == "i" {
		slices.Sort(s.valType)
		createSchema := fmt.Sprintf("CREATE SCHEMA %s;", s.dbName)
		createTable := fmt.Sprintf("CREATE TABLE %s.%s (%s);", s.dbName, s.tableName, strings.Join(s.valType, ", "))
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);", s.dbName, s.tableName, strings.Join(s.keys, ", "), strings.Join(s.vals, ", "))

		return createSchema + createTable +insertQuery
	} else if s.op == "u" {
		return fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s;", s.dbName, s.tableName, s.getUpdateClause(), s.getConditionClause())
	} else if s.op == "d" {
		return fmt.Sprintf("DELETE FROM %s.%s WHERE %s;", s.dbName, s.tableName, s.getConditionClause())
	}

	return ""
}

func(s *SqlCommand) Parse() (string, error) {
	var result map[string]interface{}
	err := json.Unmarshal([]byte(s.rawOplog), &result)
	if err != nil {
		return "", err
	}

	s.setOperationType(result)
	s.setTableName(result)
	s.setKeysAndValues(result)
	
	return s.String(), nil
}

func(s *SqlCommand) setOperationType(result map[string]interface{}) {
	if result["op"] == "i" || result["op"] == "u" || result["op"] == "d" {
		s.op = result["op"].(string)
	}
}

func(s *SqlCommand) setTableName(result map[string]interface{}) {
    if result["ns"] != nil {
        s.dbName = strings.Split(result["ns"].(string), ".")[0]
		s.tableName = strings.Split(result["ns"].(string), ".")[1]
    }
}

func(s *SqlCommand) setKeysAndValues(result map[string]interface{}) {
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

func(s *SqlCommand) getConditionClause() string {
	var conditionClause string
	for key, val := range s.conditionMap {
		conditionClause = fmt.Sprintf("%v = %v", key, s.convertValueToString(val))
	}

	return conditionClause
}

func(s *SqlCommand) getUpdateClause() string {
	var updateClause string

	for key, val := range s.setMap {
		updateClause = fmt.Sprintf("%v = %v", key, s.convertValueToString(val))
	}

	for key := range s.unsetMap {
		updateClause = fmt.Sprintf("%s = NULL", key)
	}

	return updateClause
}

func(s *SqlCommand) createTableColumn(key string, val interface{}) string {
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

func(s *SqlCommand) convertValueToString(val interface{}) string {
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