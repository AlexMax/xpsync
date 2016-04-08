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

func TestGetSince(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	rows, err := db.GetSince(1459903084.82901)
	if assert.NoError(t, err) {
		assert.Equal(t, 2, len(rows), "Should return two values")
	}

	rows, err = db.GetSince(2000000000)
	if assert.NoError(t, err) {
		assert.Equal(t, 0, len(rows), "Should return no values")
	}
}
