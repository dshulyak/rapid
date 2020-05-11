Consensus - Fast paxos
===

When changes are observed Rapid runs a round of the fast paxos to guarantee that
all replicas will agree on the same set of changes.

Based on the [Fast Paxos](#1-lamport-fast-paxos-2015) and [Fast Paxos. Made Easy](#2-zhao-fast-paxos-made-easy) that simplifies rule for choosing safe value.
And borrowing ideas for leader election and replication from [Paxos lecture from raft case study](#3-ousterhout-paxos-lecture-from-raft-study).


TLA++
---

WIP [TLA++ spec](./tla/Consensus.tla).

Messages
---

#### Prepare

```proto
message Prepare {
  uint64 ballot = 1;
  uint64 sequence = 2;
}
```

#### Promise

```proto
message Promise {
  uint64 ballot = 1;
  uint64 sequence = 2;
  uint64 voteBallot = 3;
  Value value = 4;
  uint64 commitedSequence = 5;
}

message Value {
  bytes id = 1;
  types.Changes changes = 2;
  repeated types.Node nodes = 3;
}
```

#### Accept

```proto
message Accept {
  uint64 ballot = 1;
  uint64 sequence = 2;
  Value value = 3;
}
```

#### Accepted

```proto
message Accepted {
  uint64 ballot = 1;
  uint64 sequence = 2;
  Value value = 3;
}
```

#### Learned

```proto
message LearnedValue {
  uint64 ballot = 1;
  uint64 sequence = 2;
  Value value = 3;
}

message Learned {
  repeated LearnedValue values = 1;
}
```

Protocol
---

#### Setup

Classic quorum is composed from Nodes/2 + 1.
Fast quorum is composed from ceil(3/4*Nodes).

Each node is started with randomized timer, that is equal to NETWORK_DELAY+RAND(NETWORK_DELAY/2).

Each node must store current ballot.
Each node must store last commited configuration.
Each node must store picked value and ballot when value was picked.

Coordinator maintains last commited configuration ID for every replica
.
#### Phases

###### Phase-1-A

Phase executed by coordinator.

When timer triggers replica will create [Prepare](#Prepare) message with incremented ballot and sequence.

###### Phase-1-B

Phase executed by every replica.
Starts when new **Prepare** message received.

If sequence number is less than commited value replice will reply with [Learned](#Learned) message in order to update
sender configuration.

If ballot is less then the local ballot replica will reply with [Promise](#Promise) message and local ballot.

If ballot is higher then the local ballot:
- replica updates local ballot
- replica replies with voted value and voted ballot for requested sequnce.
- replica includes last commited configuration id for the coordinator.

###### Phase-2-A

Phase is executed by coordinator.

After coordinator collected messages from Classic Quorum it will be promoted to elected, and in future
[Prepare](#Prepare) messages will be skipped.

Coordinator needs to prepare a value to accept. Value is selected according to fast paxos safety rule:
- if at most single value is selected in the messages from quorum - value must be selected.
- if majority replies in quorum selected same value - value must be selected.
- otherwise coordinator is free to pick any value.

In case if value must be selected then coordinator broadcasts [Accept](#Accept) with that value.

If any value can be selected then coordinator broadcasts special [Accept](#Accept) with value ID - **ANY**.

###### Phase-2-B

Phase is executed by every replica.

Same safety checks as in [Phase 1 B](#Phase-1-B). With a difference that message must be from the same ballot or higher.

If [Accept](#Accept) message carries concrete value replica replies immediatly with [Accepted](#Accepted) message.

If **ANY** [Accept](#Accept) message was received replica either replies with value from local proposed queue, or
waits until some value will be proposed. And once value is available sends [Accepted](#Accepted) message to coordinator.

###### Phase-3

Phase is executed by coordinator.

Coordinator aggregates messages from FastQuorum or ClassicQuorum based on the round.

If ClassicQuorum, in classic round, voted for the value then the value is commited, and coordinator broadcasts
[Learned](#Learned) message.

In FastQuorum coordinator may receive different value in [Accepted](#Accepted) messages from different replicas.
If FastQuorum voted for the same value - this value is commited and [Learned](#Learned) message is broadcasted.

Otherwise coordinator must pick either value that received votes from majority in the FastQuorum or pick any value.
With this value coordinator runs [coordinated recovery][1].

For coordinator recovery coordinator starts [Phase 2 A](#Phase-2-A) with incremented ballot and picked value. Effectively
set of [Accepted](#Accepted) messages play same role as set of [Promise](#Promise) messages in [Phase 1 B](#Phase-1-B).

References
---

###### 1. [Lamport. Fast Paxos 2015.](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf)
###### 2. [Zhao. Fast Paxos Made Easy.](https://www.researchgate.net/publication/271910927_Fast_Paxos_Made_Easy_Theory_and_Implementation)
###### 3. [Ousterhout. Paxos lecture from Raft study.](https://www.youtube.com/watch?v=JEpsBg0AO6o)