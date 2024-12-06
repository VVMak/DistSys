package main

import (
	// "bytes"
	// "encoding/json"
	// "fmt"
	// "net/http"
	"sync"
	// "time"
)

import (
	"distsys/raft/replica"
)

func main() {
	ports := []int{17423, 17424, 17425}
	replicas := []replica.Replica{}
	for _, port := range ports {
		replicas = append(replicas, replica.CreateReplica(port, ports))
	}
	var wg sync.WaitGroup
	for _, r := range replicas {
		r.Start(&wg)
	}
	// go func() {
	// 	time.Sleep(5 * time.Second)
	// 	replicas[2].Stop()
	// }()

	wg.Wait()
}
