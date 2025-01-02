package diagnostics

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
)

type DiagMessages struct {
	Type    string      `json:"type"`
	Message interface{} `json:"message"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (d *DiagnosticClient) SetupNotifier(rootCtx context.Context) {
	d.notifierChan = make(chan DiagMessages, 100)

	http.HandleFunc("/ws", d.handleConnections)

	fmt.Println("Starting server on :6063")
	if err := http.ListenAndServe(":6063", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}

	go d.Listen(rootCtx)
}

func (d *DiagnosticClient) Notify(msg DiagMessages) {
	d.notifierChan <- msg
}

func (d *DiagnosticClient) Listen(rootCtx context.Context) {
	for {
		select {
		case <-rootCtx.Done():
			close(d.notifierChan)
			return
		case msg := <-d.notifierChan:
			if d.conn != nil {
				fmt.Printf("Sending: %v\n", msg)
				if err := d.conn.WriteJSON(msg); err != nil {
					fmt.Println("Error writing message:", err)
				}
			}
		}
	}
}

func (d *DiagnosticClient) handleConnections(w http.ResponseWriter, r *http.Request) {
	var err error
	d.conn, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Error upgrading connection:", err)
		return
	}
	defer d.conn.Close()

	fmt.Println("Client connected")
}
