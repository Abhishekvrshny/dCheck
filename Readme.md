# dCheck : distributed URL checker
This is a sample golang app to demonstrate the power of zookeeper and how it can be used to implement a service like, distributed URL health checker.

`DISCLAIMER` : *This is a sample app. No code snippet to be used in production without reviewing*

## Running the app
### Get the code
```
go get github.com/Abhishekvrshny/dcheck
```
### Install the dependencies
```
go get github.com/samuel/go-zookeeper/zk
```
### Install zookeeper
Install `zookeeper` and make sure it's listening on port `2181`
#### On mac
```
brew install zookeeper
```
### Run the code
#### worker with id = 1
```
go run cmd/main.go --id=1
```
#### worker with id = 2
```
go run cmd/main.go --id=2
```
#### to add a URL from zkcli
```
create /dcheck/urls/bing.com ""
```
#### to delete a URL from zkcli
```
delete /dcheck/urls/bing.com
```
#### sample output
```
➜  dCheck git:(master) go run cmd/main.go --id=1
2019/09/05 22:15:50 Connected to [::1]:2181
2019/09/05 22:15:51 authenticated: id=72085958353616964, timeout=5000
2019/09/05 22:15:51 re-submitting `0` credentials after reconnect
LEADER : I am the Leader
LEADER : list of urls : [razorpay.com bing.com]
LEADER : list of workers : [1]
LEADER : updating data for worker : 1 : [razorpay.com bing.com]
WORKER : got updated payload : [razorpay.com bing.com]
WORKER : ID 1 : GOROUTINE 2 : checking URL bing.com
WORKER : ID 1 : GOROUTINE 1 : checking URL razorpay.com
WORKER : ID 1 : GOROUTINE 2 : checking URL bing.com
WORKER : ID 1 : GOROUTINE 1 : checking URL razorpay.com
```
 
