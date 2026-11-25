// Command serve is a tiny static file server for the web page. It exists so the
// UI can be opened with `go run ./cmd/serve` and fetch report.json/sweep.json
// without hitting a file:// CORS restriction.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8099", "listen address")
	dir := flag.String("dir", "web", "directory to serve")
	flag.Parse()

	if _, err := os.Stat(*dir); err != nil {
		fmt.Fprintf(os.Stderr, "目录 %s 不存在：先运行 go run ./cmd/sim 生成报告\n", *dir)
		os.Exit(1)
	}
	fs := http.FileServer(http.Dir(*dir))
	http.Handle("/", noCache(fs))
	log.Printf("打开 http://%s/  （Ctrl-C 停止）", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}

func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		h.ServeHTTP(w, r)
	})
}
