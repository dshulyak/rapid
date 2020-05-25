![Go](https://github.com/dshulyak/rapid/workflows/Go/badge.svg)

Rapid: membership service with consistent view changes.
===

**WORK IN PROGRESS**

Package with membership service with decentralized monitoring topology and consistent propagation of changes of the cluster configuration.

Monitoring topology is created from K graphs, where every node must monitor and monitored by K nodes.
According to [rapid technical report](#1-stable-and-consistent-membership-at-scale-with-rapid) current monitoring topology will guaranteee almost-everywhere detection. In case when majority of the cluster detects change it will take 2 network delays for the
cluster to detect new configuration. If there are conflicts it will take 3 more network delays to reach an agreement.

Service is using grpc for all network communication.

## Contents

- [Configuration](#configuration)
- [API](#api)
- [Specs](#specs)
- [License](#license)
- [References](#references)

## Configuration

```go
type Config struct {
        // Expected network delay used for ticks
        NetworkDelay StringDuration

        // Paxos

        // Timeouts are expressed in number of network delays.
        // ElectionTimeout if replica doesn't receive hearbeat for ElectionTimeout ticks
        // it will start new election, by sending Prepare to other replicas.
        ElectionTimeout int
        // HeartbeatTimeout must be lower than election timeout.
        // Leader will send last sent Accept message to every replica as a heartbeat.
        HeartbeatTimeout int

        // Monitoring

        // Timeouts are expressed in number of network delays.
        // Each observer for the same subject must reinforce other observer vote after reinforce timeout.
        ReinforceTimeout int
        // RetransmitTimeout used to re-broadcast observed alerts.
        RetransmitTimeout int

        // Connectivity is a K paramter, used for monitoring topology construction.
        // Each node will have K observers and K subjects.
        Connectivity  int
        LowWatermark  int
        HighWatermark int

        // IP and Port of the seed.
        Seed *types.Node

        // IP and Port of the instance.
        IP   string
        Port uint64

        DialTimeout, SendTimeout time.Duration
}
```

## API

**API may change during development**

Rapid instance needs to be initialized with logger, configuration and failure detector. In the example i am using
simple prober, that dials once every period and if it can't reach the node consecutively it exits with
notification. This can be replaced with application-based failure detector, phi acrual failure detector, etc.

The interface for failure detector is:

```go
type FailureDetector interface {
        Monitor(context.Context, *types.Node) error
}
```

To bootstrap a cluster, start a single seed node and connect every other node to this seed:

```go
logger := zap.NewNop()
fd := prober.New(logger.Sugar(), 2*time.Second, 2*time.Second, 3)
updates := make(chan *types.Configuration, 1)
if err := rapid.New(logger, conf, fd).Run(ctx, updates); err != nil {
   panic(err)
}
```


## Specs

Latest specs can be foudn at [specs](specs).

## License

[MIT](LICENSE) © Dmitry Shulyak

## References

###### 1. [Stable and Consistent Membership at Scale with Rapid](https://dahliamalkhi.files.wordpress.com/2019/04/rapid-atc2018.pdf)