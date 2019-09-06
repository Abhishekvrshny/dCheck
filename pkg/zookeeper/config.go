package zookeeper

import "time"

type ZKConfig struct {
	Hosts          []string
	SessionTimeout time.Duration
	RootPath       string
	RetryCount     int
	Paths          []string
	RetrySleep     time.Duration
}

type NodeNameEvent struct {
	NodeNames []string
}

func (nne *NodeNameEvent) GetNodeNames() []string {
	return nne.NodeNames
}
