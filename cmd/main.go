package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Abhishekvrshny/dCheck/internal/constants"
	"github.com/Abhishekvrshny/dCheck/internal/leader"
	"github.com/Abhishekvrshny/dCheck/internal/worker"
	"github.com/Abhishekvrshny/dCheck/pkg/zookeeper"
)

func main() {

	//var port = flag.String("port", "8989", "port")
	var id = flag.String("id", "", "id")

	flag.Parse()
	if *id == "" {
		fmt.Printf("id is required")
		os.Exit(1)
	}

	zkClient := initZK()

	ldr := leader.New(zkClient)
	err := ldr.Run()
	if err != nil {
		fmt.Printf("error running leader %s", err.Error())
		os.Exit(2)
	}
	// just give some time for leader to come up,
	// can be handled differently though
	time.Sleep(5*time.Second)
	wrkr := worker.New(zkClient, *id)
	err = wrkr.Run()
	if err != nil {
		fmt.Printf("error running worker %s", err.Error())
		os.Exit(3)
	}

	c := make(chan os.Signal, 1)
	// accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// or SIGTERM. SIGKILL, SIGQUIT will not be caught.
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	/* TODO: Take urls from a REST endpoint
	ctrlr := controller.New(zkClient)
	mux := http.NewServeMux()
	mux.HandleFunc("/check", ctrlr.Check)
	go http.ListenAndServe(fmt.Sprintf(":%s", *port), mux)
	*/
	<-c
	wrkr.Stop()
	ldr.Stop()
}

func initZK() *zookeeper.ZookeeperClient {
	zkClient, err := zookeeper.NewZKClient(&zookeeper.ZKConfig{
		Hosts:          []string{"localhost:2181"},
		SessionTimeout: 5 * time.Second,
		RootPath:       "/dcheck",
		Paths:          []string{constants.LEADERPATH, constants.WORKERSPATH, constants.URLSPATH},
		RetryCount:     3,
		RetrySleep:     2,
	})
	if err != nil {
		os.Exit(1)
	}
	return zkClient
}
