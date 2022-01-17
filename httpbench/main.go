package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/echlebek/lasr"

	_ "modernc.org/sqlite"
)

var (
	port = flag.Int("port", 8080, "Port to listen on")
	path = flag.String("path", "", "path to lasr db")
)

type Q struct {
	*lasr.Q
}

func (q Q) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	buf := bytes.Buffer{}
	if _, err := io.Copy(&buf, req.Body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := q.Send(buf.Bytes()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	flag.Parse()
	if *path == "" {
		td, err := ioutil.TempDir("", "")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(td)
		*path = filepath.Join(td, "lasr.db")
	}
	db, err := sql.Open("sqlite", *path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println(err)
		}
	}()
	q, err := lasr.NewQ(db, "test")
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		for {
			msg, err := q.Receive(context.Background())
			if err != nil {
				log.Println(err)
			}
			if err := msg.Ack(); err != nil {
				log.Println(err)
			}
		}
	}()
	handler := Q{Q: q}
	http.ListenAndServe(fmt.Sprintf(":%d", *port), handler)
}
