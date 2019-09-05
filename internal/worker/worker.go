package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Abhishekvrshny/dCheck/internal/constants"
	"github.com/Abhishekvrshny/dCheck/internal/models"
	"github.com/Abhishekvrshny/dCheck/pkg/zookeeper"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type Worker struct {
	zkClient *zookeeper.ZookeeperClient
	quitHandler func()
	id string
	controllerContext context.Context
	cancelFunc context.CancelFunc
	doneList []chan bool
	stopChan chan bool
}

func New(zkClient *zookeeper.ZookeeperClient, id string) *Worker{
	w := &Worker{zkClient:zkClient, id:id}
	w.controllerContext, w.cancelFunc = context.WithCancel(context.Background())
	return w
}

func (w *Worker) Run() error {
	wPath := w.zkClient.Config.RootPath + "/" + constants.WORKERSPATH + "/" + w.id
	_, err := w.zkClient.Create(wPath, []byte(""), zk.FlagEphemeral, zk.WorldACL(zk.PermAll), false)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	w.control(wPath)
	return nil
}

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
			err := json.Unmarshal(wData,&urls)
			if err != nil {
				fmt.Printf("error in unmarshalling")
			}
			fmt.Println(urls)
			w.checkURLs(urls.U)
		case <-w.controllerContext.Done():
			w.stopAllChecks()
			w.stopChan <- true
			return
		}
	}
}

func (w *Worker) checkURLs(urls []string) {
	for _, url := range urls {
		ticker := time.NewTicker(1 * time.Second)
		done := make(chan bool)
		w.doneList = append(w.doneList,done)
		go w.checkURL(url, ticker, done)
	}
}

func (w *Worker) checkURL(url string, ticker *time.Ticker, done chan bool) {
	for {
		select {
		case <-done:
			return
		case _ = <-ticker.C:
			fmt.Printf("Checking URL %s\n", url)
		}
	}
}

func (w *Worker) Stop() {
	w.cancelFunc()
	<- w.stopChan
}

func (w *Worker) stopAllChecks() {
	for _, d := range w.doneList {
		d <- true
	}
}

