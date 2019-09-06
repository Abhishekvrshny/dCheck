package controller
// TODO: Implement writing urls to zk here

import (
	"net/http"

	"github.com/Abhishekvrshny/dCheck/pkg/zookeeper"
)

type Controller struct {
	zClient *zookeeper.ZookeeperClient
}

func New(zClient *zookeeper.ZookeeperClient) *Controller {
	return &Controller{zClient}
}

func (c *Controller) Check(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}
