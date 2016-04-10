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

// A Float64 that is automatically converted to and from a string.
type StringInt32 int32

// Scans a string into an int32.
func (val *StringInt32) Scan(value interface{}) error {
	result, err := strconv.ParseInt(string(value.([]uint8)), 10, 32)
	if err != nil {
		return err
	}
	*val = StringInt32(result)
	return nil
}

// A Float64 that is automatically converted to and from a string.
type StringFloat64 float64

// Scans a string into an float64.
func (val *StringFloat64) Scan(value interface{}) error {
	result, err := strconv.ParseFloat(string(value.([]uint8)), 64)
	if err != nil {
		return err
	}
	*val = StringFloat64(result)
	return nil
}

// Create a new database instance.
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

// A single row of a user's experience points.
type Experience struct {
	Name       string        `db:"KeyName"`
	Experience StringInt32   `db:"Value"`
	Timestamp  StringFloat64 `db:"Timestamp"`
}

// Retrieve a single row from the database by username.
func (database *Database) Get(name string) (xp *Experience, err error) {
	xp = &Experience{}

	row := database.db.QueryRowx(`SELECT KeyName, Value, Timestamp FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = ? LIMIT 1`, name)
	err = row.StructScan(xp)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return
}

// Retrieve all rows from the database since a certain timestamp.
func (database *Database) GetSince(timestamp float64) (xps []Experience, err error) {
	rows, err := database.db.Queryx(`SELECT KeyName, Value, Timestamp FROM Zandronum WHERE Timestamp > ?`, timestamp)
	if err != nil {
		return
	}

	defer rows.Close()
	for rows.Next() {
		var xp Experience
		err = rows.StructScan(&xp)
		if err != nil {
			return
		}
		xps = append(xps, xp)
	}
	return
}

func (database *Database) Update(xp Experience) (err error) {
	// Atomically update any existing field we have in the database
	_, err = database.db.Exec(`
		INSERT OR REPLACE
		INTO Zandronum (Namespace, KeyName, Value, Timestamp)
		VALUES("zanxp", ?1,
		CASE WHEN (SELECT Timestamp FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = ?1) > ?3 THEN
			(SELECT Value FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = ?1)
		ELSE
			?2
		END,
		CASE WHEN (SELECT Timestamp FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = ?1) > ?3 THEN
			(SELECT Timestamp FROM Zandronum WHERE Namespace = "zanxp" AND KeyName = ?1)
		ELSE
			?3
		END)`, xp.Name, xp.Experience, xp.Timestamp)
	return
}
