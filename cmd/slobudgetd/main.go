// Command slobudgetd 启动一个使用 WAL 持久化的 SLO 错误预算门禁 HTTP 服务。
//
// 用法:
//
//	slobudgetd -addr :8080 -data ./data
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
	dataDir := flag.String("data", "./data", "WAL 数据目录")
	flag.Parse()

	store, err := slobudget.NewWALStore(*dataDir)
	if err != nil {
		log.Fatalf("打开存储失败: %v", err)
	}

	svc, err := slobudget.NewService(context.Background(), store)
	if err != nil {
		log.Fatalf("初始化服务失败: %v", err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           slobudget.NewHTTPHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("SLO 预算门禁服务监听 %s，数据目录 %s", *addr, *dataDir)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务退出: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("优雅关闭失败: %v", err)
	}
	if err := store.Close(); err != nil {
		log.Printf("关闭存储失败: %v", err)
	}
}
