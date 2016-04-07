package main

import (
	"database/sql"
	"io/ioutil"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	db *sqlx.DB
}

type Experience struct {
	Name       string
	Experience int32
	Timestamp  float64
}

const updateQuery string = `
INSERT OR REPLACE
INTO Zandronum(Namespace, KeyName, Value, Timestamp)
VALUES("zanxp", :Name,
	CASE WHEN (SELECT Value FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = :Name) > :Experience THEN
	(SELECT Value FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = :Name)
	ELSE
		:Experience
	END,
	COALESCE(
		(SELECT Timestamp FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = :Name),
		(SELECT (julianday('now') - 2440587.5) * 86400.0)));
`

func NewDatabase(filename string) (database *Database, err error) {
	database = &Database{}

	database.db, err = sqlx.Connect("sqlite3", filename)
	if err != nil {
		return
	}

	return
}

// Import executes a file containing SQL statements on the loaded database.
func (database *Database) Import(paths ...string) (err error) {
	for _, path := range paths {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		_, err = database.db.Exec(string(data))
		if err != nil {
			return err
		}
	}

	return
}

func (database *Database) Get(name string) (xp *Experience, err error) {
	xp = &Experience{}

	var result struct {
		KeyName   string `db:"KeyName"`
		Value     string `db:"Value"`
		Timestamp string `db:"Timestamp"`
	}

	row := database.db.QueryRowx(`SELECT KeyName, Value, Timestamp FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = ? LIMIT 1`, name)
	err = row.StructScan(&result)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return
	}

	// Convert result to Experience struct
	xp.Name = result.KeyName
	experience, err := strconv.ParseInt(result.Value, 10, 32)
	if err != nil {
		return
	}
	xp.Experience = int32(experience)
	timestamp, err := strconv.ParseFloat(result.Timestamp, 64)
	if err != nil {
		return
	}
	xp.Timestamp = timestamp
	return
}
