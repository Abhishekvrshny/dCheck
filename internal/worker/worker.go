package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/Abhishekvrshny/dCheck/internal/constants"
	"github.com/Abhishekvrshny/dCheck/internal/models"
	"github.com/Abhishekvrshny/dCheck/pkg/color"
	"github.com/Abhishekvrshny/dCheck/pkg/zookeeper"
)

type Worker struct {
	zkClient          *zookeeper.ZookeeperClient
	quitHandler       func()
	id                string
	controllerContext context.Context
	cancelFunc        context.CancelFunc
	doneList          []chan bool
	stopChan          chan bool
}

func New(zkClient *zookeeper.ZookeeperClient, id string) *Worker {
	w := &Worker{zkClient: zkClient, id: id}
	w.controllerContext, w.cancelFunc = context.WithCancel(context.Background())
	return w
}

// Run creates ephemeral node in zk for the worker and invokes control
func (w *Worker) Run() error {
	wPath := w.zkClient.Config.RootPath + "/" + constants.WORKERSPATH + "/" + w.id
	_, err := w.zkClient.Create(wPath, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll), false)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	w.control(wPath)
	return nil
}

// control loop of the worker
func (w *Worker) control(path string) {
	wCh, err := w.zkClient.WatchForever(path, w.controllerContext)
	if err != nil {
		fmt.Errorf(err.Error())
		return
	}
	for {
		select {
		case wData := <-wCh:
			w.stopAllChecks()
			urls := models.URLs{}
			err := json.Unmarshal(wData, &urls)
			fmt.Printf(color.BLUESTART)
			fmt.Printf("WORKER : got updated payload : %+v\n", urls.U)
			fmt.Printf(color.BLUEEND)
			if err != nil {
				fmt.Printf("error in unmarshalling")
			}
			w.checkURLs(urls.U)
		case <-w.controllerContext.Done():
			w.stopAllChecks()
			w.stopChan <- true
			return
		}
	}
}

func (w *Worker) checkURLs(urls []string) {
	i := 1
	// TODO: Add worker pool instead of spawning goroutines
	for _, url := range urls {
		ticker := time.NewTicker(5 * time.Second)
		done := make(chan bool)
		w.doneList = append(w.doneList, done)
		go w.checkURL(url, ticker, done, i)
		i += 1
	}
}

// checkURL is the actual task that worker does
func (w *Worker) checkURL(url string, ticker *time.Ticker, done chan bool, gid int) {
	for {
		select {
		case <-done:
			return
		case _ = <-ticker.C:
			fmt.Printf("%sWORKER : ID %s : GOROUTINE %d : checking URL %s%s\n", color.BLUESTART, w.id, gid, url, color.BLUEEND)
		}
	}
}

// Stop to gracefully shut down the worker
func (w *Worker) Stop() {
	w.cancelFunc()
	<-w.stopChan
}

// stopAllChecks is yet another helper
func (w *Worker) stopAllChecks() {
	for _, d := range w.doneList {
		d <- true
	}
	w.doneList = w.doneList[:0]
}
