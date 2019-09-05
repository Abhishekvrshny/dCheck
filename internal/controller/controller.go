package controller

import (
	"github.com/Abhishekvrshny/dCheck/pkg/zookeeper"
	"net/http"
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