package main

import (
	"net/rpc"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewApp(t *testing.T) {
	_, err := NewApp()
	assert.NoError(t, err)
}

func TestFullUpdate(t *testing.T) {
	app, err := NewApp()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = app.db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	go app.ListenAndServe()

	time.Sleep(time.Second)

	client, err := rpc.Dial("tcp", "localhost:9876")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	var nothing struct{}
	var xps []Experience
	err = client.Call("Messages.FullUpdate", nothing, &xps)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	assert.Equal(t, 5, len(xps))
}
