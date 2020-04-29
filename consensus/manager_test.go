package consensus_test

import (
	"testing"
	"time"
)

func TestManagerProgress(t *testing.T) {
	cluster := NewCluster(4, 100*time.Millisecond)
	cluster.Start()
	defer cluster.Stop()
	time.Sleep(5 * time.Second)
}
