package parser

import (
	"testing"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

func TestMongoOplogParser(t *testing.T) {
	tt := []struct {
		name string
		input string
		exp string
	}{
		// {
		// 	name: "insert statement",
		// 	input: `{
		// 		"op": "i",
		// 		"ns": "test.student",
		// 		"o": {
		// 			"_id": "635b79e231d82a8ab1de863b",
		// 			"name": "Selena Miller",
		// 			"roll_no": 51,
		// 			"is_graduated": false,
		// 			"date_of_birth": "2000-01-30"
		// 		}
		// 	}`,
		// 	exp: "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
		// },
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
		{
			name: "delete statement",
			input: `{
				"op": "d",
				"ns": "test.student",
				"o": {
					"_id": "635b79e231d82a8ab1de863b"
				}
			}`,
			exp: "DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';",
		},
		{
			name: "create table with insert statement",
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
			exp: `
				CREATE SCHEMA test;
				CREATE TABLE test.student
				(
					_id           VARCHAR(255) PRIMARY KEY,
					date_of_birth VARCHAR(255),
					is_graduated  BOOLEAN,
					name          VARCHAR(255),
					roll_no       FLOAT
				);
				INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);
			`,
		},
		{
			name: "create table with multiple insert statement",
			input: `[
				{
					"op": "i",
					"ns": "test.student",
					"o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
					}
				},
				{
					"op": "i",
					"ns": "test.student",
					"o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "George Smith",
					"roll_no": 21,
					"is_graduated": true,
					"date_of_birth": "2001-03-23"
					}
				}
			]`,
			exp: `
				CREATE SCHEMA test;
				CREATE TABLE test.student
				(
					_id           VARCHAR(255) PRIMARY KEY,
					date_of_birth VARCHAR(255),
					is_graduated  BOOLEAN,
					name          VARCHAR(255),
					roll_no       FLOAT
				);
				INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);
				INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 21);
			`,
		},
		{
			name: "create and alter table with multiple insert statement",
			input: `[
				{
					"op": "i",
					"ns": "test.student",
					"o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
					}
				},
				{
					"op": "i",
					"ns": "test.student",
					"o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "George Smith",
					"roll_no": 21,
					"is_graduated": true,
					"date_of_birth": "2001-03-23",
					"phone": "+91-81254966457"
					}
				}
			]`,
			exp: `
				CREATE SCHEMA test;
				CREATE TABLE test.student
				(
					_id           VARCHAR(255) PRIMARY KEY,
					date_of_birth VARCHAR(255),
					is_graduated  BOOLEAN,
					name          VARCHAR(255),
					roll_no       FLOAT
				);
				INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);
				ALTER TABLE test.student ADD phone VARCHAR(255);
				INSERT INTO test.student (_id, date_of_birth, is_graduated, name, phone, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', '+91-81254966457', 21);
			`,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			s := NewMongoOplogParser(tc.input)
			
			got, err := s.GetEquivalentSQL()
			if err != nil {
				t.Errorf("Error: %v", err)
			}

			result, err := compareSqlStatement(t, tc.exp, got)
			if err != nil {
				t.Fatalf("Error while comparing SQL statements: %v", err)
			}

			if !result {
				t.Errorf("Expected %q but got %q", tc.exp, got)
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
