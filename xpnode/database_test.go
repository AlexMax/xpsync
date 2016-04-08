package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDatabase(t *testing.T) {
	_, err := NewDatabase(":memory:")
	if err != nil {
		t.Error("%s", err.Error())
	}
}

func TestGet(t *testing.T) {
	db, err := NewDatabase(":memory:")
	if err != nil {
		t.Error(err.Error())
	}

	err = db.Import("../fixture/zanxp.sql")
	if err != nil {
		t.Fatal(err.Error())
	}

	row, err := db.Get("alexmax")
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, "alexmax", row.Name, "Should return proper row name")
	assert.Equal(t, StringInt32(359450), row.Experience, "Should return proper row experience")
	assert.Equal(t, StringFloat64(1459903084.82901), row.Timestamp, "Should return proper row timestamp")

	row, err = db.Get("anonymous")
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.Nil(t, row, "Should be nil")
}
