package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDatabase(t *testing.T) {
	_, err := NewDatabase(":memory:")
	assert.NoError(t, err)
}

func TestGet(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	row, err := db.Get("alexmax")
	if assert.NoError(t, err) {
		assert.Equal(t, "alexmax", row.Name, "Should return proper row name")
		assert.Equal(t, StringInt32(359450), row.Experience, "Should return proper row experience")
		assert.Equal(t, StringFloat64(1459903084.82901), row.Timestamp, "Should return proper row timestamp")
	}

	row, err = db.Get("anonymous")
	if assert.NoError(t, err) {
		assert.Nil(t, row, "Should be nil")
	}
}

func TestGetServerTimestamp(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	servertime, err := db.GetServerTimestamp("alexmax")
	if assert.NoError(t, err) {
		assert.Equal(t, StringFloat64(1459903084.82901), *servertime)
	}

	servertime, err = db.GetServerTimestamp("anonymous")
	if assert.NoError(t, err) {
		assert.Nil(t, servertime)
	}
}

func TestGetSince(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	rows, err := db.GetSince(1458872136.47299)
	if assert.NoError(t, err) {
		assert.Equal(t, 2, len(rows), "Should return two values")
	}

	rows, err = db.GetSince(2000000000)
	if assert.NoError(t, err) {
		assert.Equal(t, 0, len(rows), "Should return no values")
	}
}

func TestGetChanged(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	rows, err := db.GetChanged()
	if assert.NoError(t, err) {
		assert.Equal(t, 3, len(rows), "Should return three values")
	}
}

func TestUpdateNew(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Brand new row
	err = db.Update(Experience{
		Name:       "anonymous",
		Experience: 5000,
		Timestamp:  1460320613.0,
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	row, err := db.Get("anonymous")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, "anonymous", row.Name)
	assert.Equal(t, StringInt32(5000), row.Experience)
	assert.Equal(t, StringFloat64(1460320613.0), row.Timestamp)

	servertime, err := db.GetServerTimestamp("anonymous")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, StringFloat64(1460320613.0), *servertime)
}

func TestUpdateExistingNewer(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Update newer existing row
	err = db.Update(Experience{
		Name:       "alexmax",
		Experience: 5000,
		Timestamp:  1460320613.0,
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	row, err := db.Get("alexmax")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, "alexmax", row.Name)
	assert.Equal(t, StringInt32(5000), row.Experience)
	assert.Equal(t, StringFloat64(1460320613.0), row.Timestamp)

	servertime, err := db.GetServerTimestamp("alexmax")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, StringFloat64(1460320613.0), *servertime)
}

func TestUpdateExistingOlder(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Don't update older existing row
	err = db.Update(Experience{
		Name:       "alexmax",
		Experience: 5000,
		Timestamp:  1360320613.0,
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	row, err := db.Get("alexmax")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, "alexmax", row.Name)
	assert.Equal(t, StringInt32(359450), row.Experience)
	assert.Equal(t, StringFloat64(1459903084.82901), row.Timestamp)

	servertime, err := db.GetServerTimestamp("alexmax")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, StringFloat64(1459903084.82901), *servertime)
}
