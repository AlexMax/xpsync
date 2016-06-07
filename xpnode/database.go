package main

import (
	"database/sql"
	"io/ioutil"
	"log"
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

	// "If the table holding experience does not exist, it would be necessary
	//  to create it." -Voltaire
	_, err = database.db.Exec(`CREATE TABLE IF NOT EXISTS Zandronum(Namespace text, KeyName text, Value text, Timestamp text, PRIMARY KEY (Namespace, KeyName));`)
	if err != nil {
		return
	}

	// The Sync table, on the other hand, almost certainly does not exist
	// before running this program for the first time.  Anytime we get a
	// message from the server, we must update the timestamp in this table.
	// Zandronum itself does not touch this table, so a simple test to see
	// if the game has updated an experience column on its own is to check
	// if the ServerTimestamp equals the Zandronum Timestamp.
	_, err = database.db.Exec(`CREATE TABLE IF NOT EXISTS Sync(Namespace text, KeyName text, ServerTimestamp text, PRIMARY KEY (Namespace, KeyName));`)

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

// Retrieve a ServerTimestamp from the Sync database by username.
func (database *Database) GetServerTimestamp(name string) (timestamp *StringFloat64, err error) {
	row := database.db.QueryRowx(`SELECT ServerTimestamp FROM Sync WHERE Namespace = "zanxp" AND KeyName = ? LIMIT 1`, name)
	err = row.Scan(&timestamp)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return
}

// Retrieve all rows from the database.
func (database *Database) GetAll() (xps []Experience, err error) {
	rows, err := database.db.Queryx(`SELECT KeyName, Value, Timestamp FROM Zandronum`)
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

// Retrieve all rows whose Zandronum Timestamp is newer than the Server Timestamp
func (database *Database) GetChanged() (xps []Experience, err error) {
	rows, err := database.db.Queryx(`
		SELECT Z.KeyName, Z.Value, Z.Timestamp
		FROM Zandronum AS Z
		LEFT OUTER JOIN Sync AS S
			ON Z.Namespace = S.Namespace
			AND Z.KeyName = S.KeyName
		WHERE Z.Timestamp > S.ServerTimestamp
		OR S.ServerTimestamp IS NULL`)
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

func (database *Database) UpdateMany(xps []Experience) (err error) {
	_, err = database.db.Query("SELECT * FROM Zandronum")
	log.Printf("UpdateMany: %p %+v", database.db, err)

	database.db.Begin()

	tx, err := database.db.Beginx()
	if err != nil {
		return err
	}

	for _, xp := range xps {
		err = database.update(tx, xp)
		if err != nil {
			return
		}
	}

	err = tx.Commit()
	return
}

func (database *Database) Update(xp Experience) (err error) {
	tx, err := database.db.Beginx()
	if err != nil {
		return err
	}

	err = database.update(tx, xp)
	if err != nil {
		return
	}

	err = tx.Commit()
	return
}

// Atomically update an existing experience field if the data we have is
// newer than existing data.
func (database *Database) update(tx *sqlx.Tx, xp Experience) (err error) {
	// Update the actual experience field, if newer.
	_, err = tx.Exec(`
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
	if err != nil {
		return
	}

	// Update the server timestamp, if newer.
	_, err = tx.Exec(`
		INSERT OR REPLACE
		INTO Sync (Namespace, KeyName, ServerTimestamp)
		VALUES ("zanxp", ?1,
		CASE WHEN (SELECT ServerTimestamp FROM Sync WHERE Namespace = "zanxp" AND KeyName = ?1) > ?2 THEN
			(SELECT ServerTimestamp FROM Sync WHERE Namespace = "zanxp" AND KeyName = ?1)
		ELSE
			?2
		END);`, xp.Name, xp.Timestamp)
	if err != nil {
		return
	}

	return
}
