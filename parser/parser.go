package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)


func Parse(data string) (string, error) {
	var result map[string]interface{}

	err := json.Unmarshal([]byte(data), &result)
	if err != nil {
		return "", err
	}
	
	nestedMap := result["o"].(map[string]interface{})
	keyArr := []string{}
	valArr := []string{}

	for key, val := range nestedMap {
		keyArr = append(keyArr, key)

		switch v := val.(type) {
		case string:
			valArr = append(valArr, fmt.Sprintf("'%s'", strings.TrimSpace(v)))
		case int:
			valArr = append(valArr, strconv.Itoa(int(v)))
		case float64:
            valArr = append(valArr, strconv.Itoa(int(v)))
		case bool:
			valArr = append(valArr, strconv.FormatBool(v))
		}
	}

	res := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", 
				result["ns"], 
				strings.Join(keyArr, ", "),
				strings.Join(valArr, ", "),
			)

	return res, nil
}