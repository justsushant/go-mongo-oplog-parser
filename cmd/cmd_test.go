package reader

import (
	"os"
	"testing"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)


func TestRead(t *testing.T) {
	inputFile := "../testdata/oplog.json"
	outputFile := "../testdata/output.sql"
	exp := `
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
			UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';
			UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';
			DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';
		`

	err := Read(inputFile, outputFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	
	data, err := os.ReadFile(outputFile)
	got := string(data)
	
	result, err := compareSqlStatement(t, exp, got)
	if err != nil {
		t.Fatalf("Error while comparing SQL statements: %v", err)
	}

	if !result {
		t.Errorf("Expected %q but got %q", exp, got)
	}
}

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
