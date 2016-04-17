package main

import (
	"log"
	"net"
	"net/rpc"
)

type App struct {
	db     *Database
	server *rpc.Server
}

// Create new App
func NewApp() (app *App, err error) {
	app = &App{}

	filename := "zanxp.db"

	app.db, err = NewDatabase(filename)
	if err != nil {
		return
	}

	app.server = rpc.NewServer()

	messages := new(Messages)
	app.server.Register(messages)

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

type Messages int

func (t *Messages) FullUpdate(_ struct{}, reply *[]Experience) (err error) {
	return
}
