--------------------------- MODULE RapidFastPaxos ---------------------------

EXTENDS Integers, FiniteSets, TLC

CONSTANT Servers

CONSTANT Values

Ballot == Nat

None == CHOOSE v : v \notin Values

Classic == {i \in SUBSET(Servers) : Cardinality(i) * 2 > Cardinality(Servers)}
Fast    == {i \in SUBSET(Servers) : Cardinality(i) * 3 > Cardinality(Servers) * 2}

GetQuorum(b) == IF b = 0 THEN Fast ELSE Classic

PrepareT == "prepare"
PromiseT == "promise"
AcceptT == "accept"
AcceptedT == "accepted"

----

VARIABLES messages,   \* message history
          ballot,     \* current servers ballot
          proposed,   \* not None if some value was proposed by server
          learned,    \* not None if value was accepted by quorum of messages
          voted,      \* last value that server voted for
          votedBallot \* ballot of the round when server voted

vars == <<messages, ballot, proposed, learned, voted, votedBallot>>

Send(m) == messages' = messages \cup {m}

Select(type, b) == {m \in messages: m.type = type /\ m.ballot = b}

SelectQ(Q, type, b) == {m \in Select(type, b): m.server \in Q}

Init == /\ messages = {}
        /\ ballot = [s \in Servers |-> 0]
        /\ proposed = [s \in Servers |-> None]
        /\ learned = [s \in Servers |-> None]
        /\ voted = [s \in Servers |-> None]
        /\ votedBallot = [s \in Servers |-> 0]

----

Propose(s) == \E v \in Values:
                 /\ ballot[s] = 0
                 /\ proposed[s] = None
                 /\ proposed' = [proposed EXCEPT ![s] = v]
                 /\ voted' = [voted EXCEPT ![s] = v]
                 /\ votedBallot' = [votedBallot EXCEPT ![s] = ballot[s]]
                 /\ Send([type   |-> AcceptedT,
                          value  |-> v,
                          server |-> s,
                          ballot |-> ballot[s]
                         ])
                 /\ UNCHANGED <<ballot, learned>>

Timeout(s, next) == /\ learned[s] = None
                    /\ ~ proposed[s] = None
                    /\ next > ballot[s]
                    /\ ballot' = [ballot EXCEPT ![s] = next]
                    /\ Select(PrepareT, next) = {}
                    /\ Send([type |-> PrepareT, server |-> s, ballot |-> next])
                    /\ UNCHANGED <<proposed, learned, voted, votedBallot>>

Learn(s) == /\ learned[s] = None
            /\ \E v \in Values:
                 \E Q \in GetQuorum(ballot[s]):
                   /\ \A q \in Q: \E m \in messages:
                        /\ m.type = AcceptedT
                        /\ m.ballot = ballot[s]
                        /\ m.server = q
                        /\ m.value = v
                   /\ learned' = [learned EXCEPT ![s] = v]
                   /\ UNCHANGED <<messages, proposed, ballot, voted, votedBallot>>

Promise(s) == \E m \in messages:
                /\ m.type = PrepareT
                /\ m.ballot > ballot[s]
                /\ ballot' = [ballot EXCEPT ![s] = m.ballot]
                /\ Send([type        |-> PromiseT,
                         server      |-> s,
                         ballot      |-> m.ballot,
                         voted       |-> voted[s],
                         votedBallot |-> votedBallot[s]
                         ])
                /\ UNCHANGED <<proposed, learned, voted, votedBallot>>

SafeValue(S, v) ==
    LET highest == (CHOOSE m \in S : \A lower \in S:
                    m.votedBallot >= lower.votedBallot).votedBallot
        msgs == { m \in S: m.votedBallot = highest /\ ~ m.voted = None }
        V == {m.voted : m \in msgs}
    IN /\ v \in V
       /\ \/ Cardinality(V) = 1
          \/ \E Q \in Fast: Cardinality({m \in msgs: m.voted = v}) * 3 > Cardinality(Q)

SafeValues(S) == {v \in Values: SafeValue(S, v)}

Accept(s) == /\ learned[s] = None
             /\ ballot[s] \in Ballot
             /\ Select(AcceptT, ballot[s]) = {}
             /\ \E Q \in Classic:
                  \E S \in SUBSET SelectQ(Q, PromiseT, ballot[s]):
                    /\ \A q \in Q: \E m \in S: m.server = q
                    /\ LET safe == SafeValues(S)
                           choice == IF safe = {}
                                     THEN proposed[s]
                                     ELSE CHOOSE v \in Values: v \in safe
                       IN Send([type   |-> AcceptT,
                                value  |-> choice,
                                server |-> s,
                                ballot |-> ballot[s]])
                    /\ UNCHANGED <<proposed, learned, ballot, voted, votedBallot>>

Accepted(s) == \E m \in messages:
                  /\ m.type = AcceptT
                  /\ m.ballot >= ballot[s]
                  /\ voted' = [voted EXCEPT ![s] = m.value]
                  /\ votedBallot' = [votedBallot EXCEPT ![s] = m.ballot]
                  /\ ballot' = [ballot EXCEPT ![s] = m.ballot]
                  /\ Send([type   |-> AcceptedT,
                           value  |-> m.value,
                           server |-> s,
                           ballot |-> m.ballot])
                  /\ UNCHANGED <<learned, proposed>>

----

Next == \E s \in Servers:
           \/ Propose(s)
           \/ Learn(s)
           \/ Promise(s)
           \/ Accept(s)
           \/ Accepted(s)
           \/ \E b \in Ballot:
              \/ Timeout(s, b)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

----

Consistency == \A s1, s2 \in Servers: \/ learned[s1] = learned[s2]
                                      \/ learned[s1] = None
                                      \/ learned[s2] = None

Liveness == <>[](\A s1, s2 \in Servers: learned[s1] = learned[s2])
=============================================================================
\* Modification History
\* Last modified Sun May 31 19:41:12 EEST 2020 by dd
\* Created Fri May 29 09:06:26 EEST 2020 by dd
