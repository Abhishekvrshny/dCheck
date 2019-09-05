package main

import (
	"flag"
	"fmt"
	"github.com/Abhishekvrshny/dCheck/internal/constants"
	"github.com/Abhishekvrshny/dCheck/internal/controller"
	"github.com/Abhishekvrshny/dCheck/internal/leader"
	"github.com/Abhishekvrshny/dCheck/internal/worker"
	"github.com/Abhishekvrshny/dCheck/pkg/zookeeper"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	var port = flag.String("port", "8989", "port")
	var id = flag.String("id", "", "id")

	flag.Parse()
	if *id == "" {
		fmt.Printf("id is required")
		os.Exit(1)
	}

	zkClient := initZK()

	ctrlr := controller.New(zkClient)

	ldr := leader.New(zkClient)
	ldr.Run()
	time.Sleep(5)
	wrkr := worker.New(zkClient,*id)
	wrkr.Run()

	c := make(chan os.Signal, 1)
	// accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// or SIGTERM. SIGKILL, SIGQUIT will not be caught.
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	mux := http.NewServeMux()
	mux.HandleFunc("/check", ctrlr.Check)

	go http.ListenAndServe(fmt.Sprintf(":%s", *port), mux)
	<-c
	wrkr.Stop()
	ldr.Stop()
}

func initZK() *zookeeper.ZookeeperClient {
	zkClient, err := zookeeper.NewZKClient(&zookeeper.ZKConfig{
		Hosts:          []string{"localhost:2181"},
		SessionTimeout: 5*time.Second,
		RootPath:       "/dcheck",
		Paths:          []string{constants.LEADERPATH, constants.WORKERSPATH, constants.URLSPATH},
		RetryCount:     2,
		RetrySleep:     1,
	})
	if err != nil {
		os.Exit(1)
	}
	return zkClient
}
