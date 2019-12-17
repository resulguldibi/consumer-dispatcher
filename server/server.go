package server

import (
	"github.com/labstack/echo"
	"os"
	"time"
)

type Server struct {
	server *echo.Echo
}

func NewServer() *Server {
	return &Server{server: echo.New()}
}

func (server *Server) Serve(address string, quitSignalChannel chan os.Signal, timeout time.Duration) {

	go func() {
		server.server.Logger.Fatal(server.server.Start(address))
	}()

	<-quitSignalChannel

	//ctx, cancel := context.WithTimeout(context.Background(), timeout)
	//defer cancel()
	//if err := server.server.Shutdown(ctx); err != nil {
	//	server.server.Logger.Fatal(err)
	//}

	//fmt.Println("server Shutdown")
}

func (server *Server) GET(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) {
	server.server.GET(path, h, m...)
}

func (server *Server) POST(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) {
	server.server.POST(path, h, m...)
}
