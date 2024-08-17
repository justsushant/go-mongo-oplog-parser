package parser

import (
	"testing"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

func TestSqlCommandParse(t *testing.T) {
	tc := []struct {
		name string
		input string
		exp string
	}{
		{
			name: "insert statement",
			input: `{
				"op": "i",
				"ns": "test.student",
				"o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
				}
			}`,
			exp: "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
		},
		{
			name: "update statement set operation",
			input: `{
				"op": "u",
				"ns": "test.student",
				"o": {
					"$v": 2,
					"diff": {
						"u": {
							"is_graduated": true
						}
					}	
				},
					"o2": {
					"_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			exp: "UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';",
		},
		{
			name: "update statement unset operation",
			input: `{
				"op": "u",
				"ns": "test.student",
				"o": {
					"$v": 2,
					"diff": {
						"d": {
							"roll_no": false
						}
					}
				},
				"o2": {
					"_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			exp: "UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			s := &SqlCommand{rawOplog: tt.input}
			
			got, err := s.Parse()
			if err != nil {
				t.Errorf("Error: %v", err)
			}

			result, err := compareSqlStatement(t, tt.exp, got)
			if err != nil {
				t.Fatalf("Error while comparing SQL statements: %v", err)
			}

			if !result {
				t.Errorf("Expected %q but got %q", tt.exp, got)
			}
		})
	}
}

// compares if sql statements are equal on the basis of fingerprint
// if they are equivalent, fingerprint will be same
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