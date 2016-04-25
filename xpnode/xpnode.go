package main

import (
	"log"
	"net"
	"net/rpc"
)

type App struct {
	db     *Database
	server *rpc.Server
	msg    *Messages
}

// Create new App
func NewApp() (app *App, err error) {
	app = &App{}

	filename := ":memory:"

	// Initialize Database
	app.db, err = NewDatabase(filename)
	if err != nil {
		return
	}

	// Initialize RPC server
	app.server = rpc.NewServer()

	// Initialize RPC messages
	app.msg = &Messages{
		app: app,
	}
	app.server.Register(app.msg)

	return
}

// Serve application
func (app *App) ListenAndServe() (err error) {
	listener, err := net.Listen("tcp", ":9876")
	if err != nil {
		return
	}
	defer listener.Close()

	log.Printf("Now listening on %s", listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		app.server.ServeConn(conn)
	}

	return
}

func main() {
	app, err := NewApp()
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Fatal(app.ListenAndServe())
}

type Messages struct {
	app *App
}

// Send a complete list of experience data to client.
func (m *Messages) FullUpdate(_ struct{}, reply *[]Experience) (err error) {
	*reply, err = m.app.db.GetAll()
	return
}

// Update local data based on data from client.
func (m *Messages) Push(xps []Experience, reply *bool) (err error) {
	err = m.app.db.UpdateMany(xps)
	if err != nil {
		*reply = false
	} else {
		*reply = true
	}
	return
}
