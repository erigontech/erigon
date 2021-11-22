package server

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/tx_analysis/manager"
)

var indexTemplate = template.Must(template.New("index").Parse(`
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Document</title>
	<script crossorigin src="https://unpkg.com/react@17/umd/react.production.min.js"></script>
	<script crossorigin src="https://unpkg.com/react-dom@17/umd/react-dom.production.min.js"></script>
</head>

<body>
</body>

</html>
`))

var address = "localhost:12345"

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == fmt.Sprintf("http://%s", address) || origin == "http://localhost:5500" // for dev purposes
	},
}

type Server struct {
	httpServer *http.Server
	reports    <-chan *manager.Report
	conn       *websocket.Conn
}

func NewServer(reports <-chan *manager.Report) *Server {

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleHTTP)

	httpServer := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	srv := &Server{
		httpServer: httpServer,
		reports:    reports,
	}

	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWS(w, r, srv)
	})

	return srv
}

func (s *Server) Run(stop chan bool) {
	go func() {
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// for report := range s.reports {
	// 	fmt.Println("report received: ", report)
	// }

	is_stop := false
	for {
		select {
		case report := <-s.reports:
			fmt.Println("WS: received report", report)

		case <-stop:
			is_stop = true
			s.stop()
			return
		default:
			if s.conn != nil && !is_stop {
				mt, message, err := s.conn.ReadMessage()
				if err != nil {
					log.Println("read:", err)
					break
				}
				log.Printf("recv: %s", message)
				err = s.conn.WriteMessage(mt, message)
				if err != nil {
					log.Println("write:", err)
					return
				}
			}
		}

	}
}

func (s *Server) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.conn != nil {
		s.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
			websocket.CloseNormalClosure, ""))
		s.conn.Close()
	}
	s.httpServer.Shutdown(ctx)
}

func handleHTTP(w http.ResponseWriter, r *http.Request) {
	indexTemplate.Execute(w, nil)
}

func handleWS(w http.ResponseWriter, r *http.Request, srv *Server) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}

	srv.conn = conn
}
