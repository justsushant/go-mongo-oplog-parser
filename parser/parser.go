package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type SqlCommand struct {
	rawOplog string
	op string
	tableName string
	keys []string
	vals []string
}

func(s *SqlCommand) String() string {
	if s.op == "i" {
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", s.tableName, strings.Join(s.keys, ", "), strings.Join(s.vals, ", "))
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

func (s *SqlCommand) setOperationType(result map[string]interface{}) {
    if result["op"] == "i" {
        s.op = "i"
    }
}

func (s *SqlCommand) setTableName(result map[string]interface{}) {
    if result["ns"] != nil {
        s.tableName = result["ns"].(string)
    }
}

func (s *SqlCommand) setKeysAndValues(result map[string]interface{}) {
    nestedMap := result["o"].(map[string]interface{})
    s.keys = make([]string, 0, len(nestedMap))
    s.vals = make([]string, 0, len(nestedMap))

    for key, val := range nestedMap {
        s.keys = append(s.keys, key)
        s.vals = append(s.vals, convertValueToString(val))
    }
}

func convertValueToString(val interface{}) string {
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