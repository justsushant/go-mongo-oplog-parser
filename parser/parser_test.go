package parser

import (
	"testing"
	pgquery "github.com/pganalyze/pg_query_go/v5"
)

func TestParse(t *testing.T) {
	input := `{
		"op": "i",
		"ns": "test.student",
		"o": {
			"_id": "635b79e231d82a8ab1de863b",
			"name": "Selena Miller",
			"roll_no": 51,
			"is_graduated": false,
			"date_of_birth": "2000-01-30"
		}
	}`

	exp := "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);"

	got, err := Parse(input)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	result, err := compareSqlStatement(t, exp, got)
	if err != nil {
		t.Fatalf("Error while comparing SQL statement: %v", err)
	}

	if !result {
		t.Errorf("Expected %s but got %s", exp, got)
	}
}

// compares if sql statements are equal on the basis of fingerprint
func compareSqlStatement(t *testing.T, expected, got string) (bool, error) {
	t.Helper()

	expFp, err := pgquery.Fingerprint(expected)
	if err != nil {
		return false, err
	}

	gotFp, err := pgquery.Fingerprint(got)
	if err != nil {
		return false, err
	}

	return expFp == gotFp, nil
}