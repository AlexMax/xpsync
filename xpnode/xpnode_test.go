package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/gorpc"
)

func TestNewApp(t *testing.T) {
	_, err := NewApp()
	assert.NoError(t, err)
}

func TestUpdate(t *testing.T) {
	app, err := NewApp()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = app.service.db.Import("../fixture/zanxp.sql")
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = app.Start()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer app.Stop()

	client := gorpc.NewTCPClient("localhost:9876")
	client.Start()
	defer client.Stop()

	dis := gorpc.NewDispatcher()
	dis.AddService("XPNodeService", app.server)
	disClient := dis.NewServiceClient("XPNodeService", client)

	// FIXME: This appears to work...
	var xps = []Experience{
		{Name: "AlexMax", Experience: 360000, Timestamp: 1500000000.0},
	}
	err = app.service.db.UpdateMany(xps)

	// ...but this fails!
	_, err = disClient.Call("Update", xps)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
}
