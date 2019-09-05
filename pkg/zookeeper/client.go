package zookeeper

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ZookeeperClient struct {
	conn         *zk.Conn
	eventChannel <-chan zk.Event
	Config       *ZKConfig
}

func NewZKClient(config *ZKConfig) (*ZookeeperClient, error) {
	conn, eventChannel, err := zk.Connect(config.Hosts, config.SessionTimeout)
	if err != nil {
		return nil, fmt.Errorf("zookeeper connect failed : %s", err.Error())
	}

	zkClient := &ZookeeperClient{conn, eventChannel, config}

	return zkClient, nil
}

func (zClient *ZookeeperClient) Get(path string) ([]byte, *zk.Stat, error) {
	b, stat, err := zClient.conn.Get(path)
	if err != nil {
		return b, stat, fmt.Errorf("zookeeper get failed with error %s", err.Error())
	}
	return b, stat, nil
}

func (zClient *ZookeeperClient) LeaderElection(path string, startFunc, stopFunc func()) (quitHandler func(), err error) {
	exists, err := zClient.Exists(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		_, err = zClient.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll), true)
		if err != nil && err != zk.ErrNodeExists {
			return nil, fmt.Errorf( "zookeeper unable to create LeaderElection path %s, error %s", path, err.Error())
		}
	}

	var quit, exit chan bool
	quit = make(chan bool)
	exit = make(chan bool)

	go func() {
		var children []string
		var exist bool
		var eventChan <-chan zk.Event
		var event zk.Event
		var isLeader bool = false

		retryCount := 0

	CreateSelfNode:
		if err != nil {
			if retryCount < zClient.Config.RetryCount {
				retryCount += 1
				goto CreateSelfNode
			} else {
				os.Exit(100)
			}
		}
		retryCount = 0
		selfPath, zErr := zClient.conn.Create(path+"/n_", []byte(""), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
		if zErr != nil {
			fmt.Printf("zookeeper error in creating node path with error %s", zErr.Error())
			if retryCount < zClient.Config.RetryCount {
				retryCount += 1
				time.Sleep(time.Second)
				goto CreateSelfNode
			} else {
				os.Exit(101)
			}
		}
		retryCount = 0
	ComputeWatchNode:
		var nodeToWatch = selfPath
		children, _, zErr = zClient.conn.Children(path)
		if zErr != nil {
			fmt.Printf("zookeeper error in getting children for path %s, error %s", path, zErr.Error())
			if retryCount < zClient.Config.RetryCount {
				retryCount += 1
				time.Sleep(time.Second)
				goto ComputeWatchNode
			} else {
				if zErr = zClient.conn.Delete(selfPath, -1); zErr != nil && zErr != zk.ErrNoNode {
					fmt.Printf("zookeeper error deleting path %s", selfPath)
				}
					os.Exit(102)
			}
		}
		retryCount = 0
		sort.Strings(children)
		for _, child := range children {
			if path+"/"+child == selfPath {
				break
			}
			nodeToWatch = path + "/" + child
		}

	Watch:
		exist, _, eventChan, zErr = zClient.conn.ExistsW(nodeToWatch)
		if !exist {
			fmt.Printf("zookeeper nodeToWatch %s doesn't exist", nodeToWatch)
			goto ComputeWatchNode
		}
		if zErr != nil {
			fmt.Printf("zookeeper error in ExistsW for path %s, error %s", nodeToWatch, zErr.Error())
			if retryCount < zClient.Config.RetryCount {
				retryCount += 1
				time.Sleep(time.Second)
				goto Watch
			} else {
				if zErr = zClient.conn.Delete(selfPath, -1); zErr != nil && zErr != zk.ErrNoNode {
					fmt.Printf("zookeeper error deleting path %s", selfPath)
				}
				os.Exit(103)
			}
		}
		retryCount = 0
		if selfPath == nodeToWatch {
			startFunc()
			isLeader = true
		}

	WaitOnEvent:
		select {
		case event = <-eventChan:
			if event.Type == zk.EventNodeDeleted {
				if isLeader {
					stopFunc()
					goto CreateSelfNode
				}
				goto ComputeWatchNode
			} else {
				goto WaitOnEvent
			}
		case <-quit:
			fmt.Printf("zookeeper stopping leader")
			if isLeader {
				stopFunc()
			}
			if zErr = zClient.conn.Delete(selfPath, -1); zErr != nil && zErr != zk.ErrNoNode {
				fmt.Printf("zookeeper error deleting path %s", selfPath)
			}
			exit <- true
			return
		}
	}()

	return func() {
		quit <- true
		<-exit
	}, nil

}

func (zClient *ZookeeperClient) Exists(path string) (bool, error) {
	ok, _, err := zClient.conn.Exists(path)
	if err != nil {
		return false, fmt.Errorf( "zookeeper exists %s failed with error %s", path, err.Error())
	}
	return ok, nil
}

func (zClient *ZookeeperClient) Create(path string, data []byte, flags int32, acl []zk.ACL, createParentContainerNodes bool) (string, error) {
	var err error
	var intermediatePath = ""
	var exists bool

	exists, err = zClient.Exists(path)
	if err != nil {
		return "", fmt.Errorf( "zookeeper exists %s failed with error %s", path, err.Error())
	}

	if exists {
		return "", fmt.Errorf( "zookeeper node already exists")
	}

	if createParentContainerNodes {
		allNodesInPath := strings.Split(path, "/")
		for _, eachNode := range allNodesInPath[:len(allNodesInPath)-1] {
			if eachNode != "" {
				intermediatePath = fmt.Sprint(intermediatePath, "/", eachNode)

				ok, err := zClient.Exists(intermediatePath)
				if err != nil {
					return "", err
				}

				if !ok {
					intermediatePath, err := zClient.conn.Create(intermediatePath, []byte(""), 0, zk.WorldACL(zk.PermAll))
					if err != nil {
						return "", fmt.Errorf( "zookeeper %s create failed with error %s", intermediatePath, err.Error())
					}
				}
			}
		}
	}

	str, err := zClient.conn.Create(path, data, flags, acl)
	if err != nil {
		return "", fmt.Errorf( "zookeeper %s create failed with error %s", path, err.Error())
	}
	return str, nil
}

func (zClient *ZookeeperClient) NodeNameW(path string, nameContext context.Context) (<-chan NodeNameEvent, error) {
	updateChannel := make(chan NodeNameEvent, 5)

	/* Fetch Children Names */
	if nodes, _, ch, err := zClient.ChildrenW(path); err == nil {
		/* Perform Initial Update of Child Nodes */
		updateChannel <- NodeNameEvent{nodes}

		/* Start Routine to Run Continuous Watch on Path */
		go func() {
			for {
				select {
				/* Fetch Updates on Children Change */
				case event := <-ch:
					switch event.Type {
					case zk.EventNodeDeleted:
						close(updateChannel)
						return
					case zk.EventNodeChildrenChanged:
						break
					}

					/* Always Renew Event Channel after Use */
					if nodes, _, ch, err = zClient.ChildrenW(path); err == nil {
						updateChannel <- NodeNameEvent{nodes}
					} else {
						close(updateChannel)
						return
					}
				case <-nameContext.Done():
					return
				}
			}
		}()

	} else {
		return nil, err
	}

	return updateChannel, nil
}

func (zClient *ZookeeperClient) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	ch, stat, event, err := zClient.conn.ChildrenW(path)
	if err != nil {
		return nil, nil, nil, fmt.Errorf( "zookeeper childrenW failed with err %s", err.Error())
	}
	return ch, stat, event, nil
}

func (zClient *ZookeeperClient) Update(path string, data []byte, version int32) (*zk.Stat, error) {
	ok, err := zClient.Exists(path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("zookeeper path %s does not exist", path)
	}
	stat, err1 := zClient.conn.Set(path, data, version)
	if err1 != nil {
		return nil, fmt.Errorf("zookeeper set failed on path %s with error %s", path, err1.Error())
	}
	return stat, nil
}

func (zClient *ZookeeperClient) WatchForever(path string, nameContext context.Context) (chan []byte, error) {
	var currentVersion int32
	var data []byte
	var stat *zk.Stat

	channel := make(chan []byte)
	data, stat, err := zClient.conn.Get(path)
	if err != nil {
		fmt.Errorf("Unable to Fetch Node Data")
		return nil, fmt.Errorf( "zookeeper get failed with error %s", err.Error())
	}
	if !bytes.Equal([]byte(""), data) {
		go func() { channel <- data }()
	}
	currentVersion = stat.Version
	go func() {
		for {
			data, stat, ch, err := zClient.conn.GetW(path)
			if err != nil {
				fmt.Errorf("Unable to set get watch on %s", path)
				os.Exit(300)
			}
			if stat.Version != currentVersion {
				channel <- data
				currentVersion = stat.Version
			}
			select {
			case <-ch:
				continue
			case <-nameContext.Done():
				fmt.Println("zookeeper WatchForever nameContext done")
				return

			}
		}
	}()
	return channel, nil
}
