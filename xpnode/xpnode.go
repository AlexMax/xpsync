package main

import (
	"log"

	"github.com/valyala/gorpc"
)

type XPNodeService struct {
	db *Database
}

func NewXPNodeService() (service *XPNodeService, err error) {
	service = &XPNodeService{}

	// Register types for service
	gorpc.RegisterType(&Experience{})

	// Initialize Database
	service.db, err = NewDatabase(":memory:")
	if err != nil {
		return
	}

	return
}

// Update experience on node from master node.
func (service *XPNodeService) Update(xps []Experience) (err error) {
	err = service.db.UpdateMany(xps)
	return
}

type App struct {
	server  *gorpc.Server
	service *XPNodeService
}

func NewApp() (app *App, err error) {
	app = &App{}

	// Initialize Service
	app.service, err = NewXPNodeService()
	if err != nil {
		return
	}

	// Initialize Server
	dis := gorpc.NewDispatcher()
	dis.AddService("XPNodeService", app.service)
	app.server = gorpc.NewTCPServer(":9876", dis.NewHandlerFunc())

	return
}

// Start the server
func (app *App) Start() (err error) {
	err = app.server.Start()
	return
}

// Stop the server
func (app *App) Stop() {
	app.server.Stop()
	return
}

func main() {
	// Create the server instance
	app, err := NewApp()
	if err != nil {
		log.Fatal(err.Error())
	}

	// Start the server
	err = app.Start()
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Fatalf("Now listening on %s", app.server.Addr)
	defer app.Stop()

	// Sleep forever
	select {}
}
