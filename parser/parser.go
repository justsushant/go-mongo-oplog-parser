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
	setArr map[string]interface{}
	unsetArr map[string]interface{}
	condition map[string]interface{}
}

func(s *SqlCommand) String() string {
	if s.op == "i" {
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", s.tableName, strings.Join(s.keys, ", "), strings.Join(s.vals, ", "))
	} else if s.op == "u" {
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s;", s.tableName, s.getUpdateClause(), s.getConditionClause())
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
    } else if result["op"] == "u" {
		s.op = "u"
	}
}

func (s *SqlCommand) setTableName(result map[string]interface{}) {
    if result["ns"] != nil {
        s.tableName = result["ns"].(string)
    }
}

func (s *SqlCommand) setKeysAndValues(result map[string]interface{}) {
    nestedMap := result["o"].(map[string]interface{})
   	if s.op == "i" {
		s.keys = make([]string, 0, len(nestedMap))
		s.vals = make([]string, 0, len(nestedMap))
	
		for key, val := range nestedMap {
			s.keys = append(s.keys, key)
			s.vals = append(s.vals, convertValueToString(val))
		}
	} else if s.op == "u" {
		nestedMap = result["o"].(map[string]interface{})["diff"].(map[string]interface{})

		if nestedMap["u"] != nil {
			s.setArr = make(map[string]interface{})
			for key, val := range nestedMap["u"].(map[string]interface{}) {
				s.setArr[key] = val
			}
		}

		if nestedMap["d"] != nil {
			s.unsetArr = make(map[string]interface{})
			for key, val := range nestedMap["d"].(map[string]interface{}) {
				s.unsetArr[key] = val
			}
		}

		if result["o2"] != nil {
			s.condition = make(map[string]interface{})
			for key, val := range result["o2"].(map[string]interface{}) {
				s.condition[key] = val
			}
		}
	}
}

func (s *SqlCommand) getConditionClause() string {
	conditionArr := make([]string, 0, len(s.condition))
	for key, val := range s.condition {
		conditionArr = append(conditionArr, fmt.Sprintf("%v = %v", key, convertValueToString(val)))
	}

	return strings.Join(conditionArr, " AND ")
}

func (s *SqlCommand) getUpdateClause() string {
	updateArr := make([]string, 0, len(s.setArr) + len(s.unsetArr))

	for key, val := range s.setArr {
		updateArr = append(updateArr, fmt.Sprintf("%v = %v", key, convertValueToString(val)))
	}

	for key := range s.unsetArr {
		updateArr = append(updateArr, fmt.Sprintf("%s = NULL", key))
	}

	return strings.Join(updateArr, ", ")
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