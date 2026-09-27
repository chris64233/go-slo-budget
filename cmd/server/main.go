// Command slo-budget-server 启动 SLO 错误预算与发布门禁的 HTTP 服务。
//
// 用法：
//
//	slo-budget-server -addr :8080 -state ./data/state.json
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	slobudget "github.com/chris64233/go-slo-budget"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	statePath := flag.String("state", "data/state.json", "状态持久化文件路径")
	flag.Parse()

	store := slobudget.NewFileStore(*statePath)
	svc := slobudget.NewService(store)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           slobudget.NewServer(svc),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("slo-budget server listening on %s (state=%s)", *addr, *statePath)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
