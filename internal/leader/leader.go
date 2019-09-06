package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/Abhishekvrshny/dCheck/internal/constants"
	"github.com/Abhishekvrshny/dCheck/internal/models"
	"github.com/Abhishekvrshny/dCheck/pkg/color"
	"github.com/Abhishekvrshny/dCheck/pkg/zookeeper"
)

type Leader struct {
	zkClient          *zookeeper.ZookeeperClient
	quitHandler       func()
	controllerContext context.Context
	cancelFun         context.CancelFunc
	workers           []string
	urls              []string
	shutdownChan      chan bool
}

func New(zkClient *zookeeper.ZookeeperClient) *Leader {
	l := &Leader{zkClient: zkClient}
	l.controllerContext, l.cancelFun = context.WithCancel(context.Background())
	return l
}

func (l *Leader) Run() error {
	leaderPath := l.zkClient.Config.RootPath + "/" + constants.LEADERPATH
	quitHandler, err := l.zkClient.LeaderElection(leaderPath, l.LeaderStart, l.LeaderStop)
	if err != nil {
		return err
	}
	l.quitHandler = quitHandler
	return nil
}

// LeaderStart runs when leader is elected
func (l *Leader) LeaderStart() {
	err := l.createBasePaths()
	if err != nil {
		l.quitHandler()
	}
	go l.control()
}

// LeaderStop runs when leader is stopped
func (l *Leader) LeaderStop() {
	l.cancelFun()
}

// createBasePaths is preparatory step to get all root paths created
func (l *Leader) createBasePaths() error {
	for path, _ := range l.zkClient.Config.Paths {
		absPath := l.zkClient.Config.RootPath + "/" + l.zkClient.Config.Paths[path]
		exists, err := l.zkClient.Exists(absPath)
		if err != nil {
			return err
		} else if !exists {
			_, err := l.zkClient.Create(absPath, []byte(""), 0, zk.WorldACL(zk.PermAll), true)
			if err != nil && err.Error() != "zookeeper node already exists" {
				return err
			}
		}
	}
	return nil
}

// control is where actual control loop of the leader runs
func (l *Leader) control() {
	fmt.Printf(color.YELLOWSTART)
	fmt.Printf("%sLEADER : I am the Leader%s\n", color.YELLOWSTART, color.YELLOWEND)
	fmt.Printf(color.YELLOWEND)
	workerCh, err := l.setPathWatchWithRetries(l.zkClient.Config.RootPath + "/" + constants.WORKERSPATH)
	if err != nil {
		fmt.Printf("Unable to set watch on %s", constants.WORKERSPATH)
		os.Exit(200)
	}
	urlCh, err := l.setPathWatchWithRetries(l.zkClient.Config.RootPath + "/" + constants.URLSPATH)
	if err != nil {
		fmt.Printf("Unable to set watch on %s", constants.URLSPATH)
		os.Exit(201)
	}
	for {
		select {
		case workerEvent := <-workerCh:
			l.workers = workerEvent.GetNodeNames()
			fmt.Printf("%sLEADER : list of workers : %v%s\n", color.YELLOWSTART, l.workers, color.YELLOWEND)
			l.distributeURLs()
			break
		case urlEvent := <-urlCh:
			l.urls = urlEvent.GetNodeNames()
			fmt.Printf("%sLEADER : list of urls : %v%s\n", color.YELLOWSTART, l.urls, color.YELLOWEND)
			l.distributeURLs()
			break
		case <-l.controllerContext.Done():
			l.quitHandler()
			l.shutdownChan <- true
			return
		}
	}
}

// setPathWatchWithRetries helper function over zk client
func (l *Leader) setPathWatchWithRetries(path string) (<-chan zookeeper.NodeNameEvent, error) {
	retryTicker := time.NewTicker(l.zkClient.Config.RetrySleep * time.Second)
	timeout := time.After(time.Duration(l.zkClient.Config.RetryCount) * l.zkClient.Config.RetrySleep * time.Second)

	nameContext, _ := context.WithCancel(l.controllerContext)

	for {
		select {
		case <-retryTicker.C:
			if ch, err := l.zkClient.NodeNameW(path, nameContext); err == nil {
				return ch, nil
			}
		case <-timeout:
			return nil, fmt.Errorf("timeout setting watch on worker directory")
		}
	}
}

// distributeURLs is some really dumb way to distribute urls amongst workers, but it works!
func (l *Leader) distributeURLs() {
	if len(l.workers) == 0 {
		return
	}
	workerData := make(map[string]models.URLs, len(l.workers))
	for i := 0; i < len(l.urls); i++ {
		targetWorker := i % len(l.workers)
		if _, ok := workerData[l.workers[targetWorker]]; !ok {
			workerData[l.workers[targetWorker]] = models.URLs{[]string{l.urls[i]}}
		} else {
			urls := append(workerData[l.workers[targetWorker]].U, l.urls[i])
			workerData[l.workers[targetWorker]] = models.URLs{urls}
		}
	}
	if len(l.urls) < len(l.workers) {
		for i := len(l.urls); i < len(l.workers); i++ {
			workerData[l.workers[i]] = models.URLs{[]string{}}
		}
	}

	workerPathPrefix := l.zkClient.Config.RootPath + "/" + constants.WORKERSPATH

	for k, v := range workerData {
		workerPath := workerPathPrefix + "/" + k
		newData, _ := json.Marshal(v)
		oldData, stat, err := l.zkClient.Get(workerPath)
		if err != nil {
			fmt.Printf("LEADER : updateAssignment: Error in updating worker, %s", err.Error())
		}
		if bytes.Compare(newData, oldData) != 0 {
			fmt.Printf("%sLEADER : updating data for worker : %+v : %+v%s\n", color.YELLOWSTART, k, v.U, color.YELLOWEND)
			_, err = l.zkClient.Update(workerPath, newData, stat.Version)
			if err != nil {
				fmt.Printf("updateAssignment: Error in updating worker, %s\n", err.Error())
			}
		} else {
			fmt.Printf("%sLEADER : not updating data for worker : %+v%s\n", color.YELLOWSTART, k, color.YELLOWEND)
		}

	}
}

// Stop gracefully shuts down the node
func (l *Leader) Stop() {
	l.cancelFun()
	<-l.shutdownChan
}
