package main

import (
	"log"
	"net"
	"net/rpc"
	"time"
)

type App struct {
	Error    chan error
	shutdown chan bool
	db       *Database
	server   *rpc.Server
	msg      *Messages
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

	// Initialize channels
	app.Error = make(chan error)
	app.shutdown = make(chan bool)

	return
}

// Start serving application
func (app *App) Start() (err error) {
	listener, err := net.Listen("tcp", ":9876")
	if err != nil {
		return
	}
	err = listener.(*net.TCPListener).SetDeadline(time.Now().Add(200 * time.Millisecond))
	if err != nil {
		return
	}

	log.Printf("Now listening on %s", listener.Addr().String())

	go func() {
		defer listener.Close()

		for {
			select {
			case <-app.shutdown:
				return
			default:
				// Do nothing
			}

			conn, err := listener.Accept()
			if err != nil {
				opErr, ok := err.(*net.OpError)
				if ok && opErr.Timeout() {
					continue
				}

				app.Error <- err
				return
			}

			app.server.ServeConn(conn)
		}
	}()

	return
}

func (app *App) Shutdown() {
	app.shutdown <- true

	close(app.shutdown)
	close(app.Error)
}

func main() {
	app, err := NewApp()
	if err != nil {
		log.Fatal(err.Error())
	}

	err = app.Start()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer app.Shutdown()

	log.Fatal(<-app.Error)
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
