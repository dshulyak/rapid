----------------------------- MODULE Consensus -----------------------------

EXTENDS Integers, FiniteSets

\* set of server ids
CONSTANTS Servers

\* set of values that can be proposed
CONSTANTS Values

CONSTANTS Coordinator, Candidate, Replica

Rounds == Nat

----

\* current round for each server
VARIABLE round

\* vote for each server
VARIABLE vote

\* round of the vote for each server
VARIABLE voteRound

\* all transmitted messages
VARIABLE messages

\* state of the each server (Coordinator/Candidate/Replica)
VARIABLE state

\* set of fast rounds
VARIABLE fastRounds

----

vars == <<round, vote, voteRound, messages, state, fastRounds>>

----

ANY == CHOOSE v : v \notin Values
NONE == CHOOSE v : v \notin Values \cup {ANY}

Max(S) == CHOOSE i \in S : \A j \in S : j \leq i

Classic == {s \in SUBSET Servers: Cardinality(s) * 2 > Cardinality(Servers)}
Fast == {s \in SUBSET Servers: Cardinality(s)  * 4 >= Cardinality(Servers) * 3 }

Quorums(r) == IF r \in fastRounds THEN Classic ELSE Fast

Send(msg) == messages' = messages \cup {msg}

AddToFast(r) == fastRounds' = fastRounds \cup {r}

Select(t, r) == {m \in messages: m.type = t /\ m.round = r}

----

Init == /\ messages = {}
        /\ round = [s \in Servers |-> 0]
        /\ vote = [s \in Servers |-> NONE]
        /\ voteRound = [s \in Servers |-> 0]
        /\ state = [s \in Servers |-> Replica]
        /\ fastRounds = {}

----

Phase1A(s, r) == /\ Select("propose", r) = {}
                 /\ Send([type |-> "propose", round |-> r, replica |-> s])
                 /\ state' = [state EXCEPT ![s] = Candidate]
                 /\ UNCHANGED <<round, vote, voteRound, fastRounds>>

Phase1B(s) == \E m \in messages:
                 /\ m.type = "propose"
                 /\ m.round > round[s]
                 /\ round' = [round EXCEPT ![s] = m.round]
                 /\ Send([type |-> "promise", round |-> m.round, vote |-> vote[s], voteRound |-> voteRound[s], replica |-> s])
                 /\ UNCHANGED <<vote, voteRound, state, fastRounds>>


ValueForAccept(S, v) ==
    LET k == Max({m.voted: m \in S})
        maxmsgs == {m \in S: m.ballot = k}
        V == {m.value: m \in maxmsgs}
    IN IF \/ Cardinality(V) = 1 /\ NONE \notin V
          \/ Cardinality({m \in maxmsgs: m.value = v})*2 > Cardinality(maxmsgs)
       THEN v \in V
       ELSE v = ANY


RenounceLeadership(s) == /\ state[s] = Coordinator
                         /\ \E m \in messages:
                             /\ m.round > round[s]
                             /\ round' = [round EXCEPT ![s] = m.round]
                         /\ UNCHANGED <<vote, voteRound, state, fastRounds, messages>>

Phase2A(s, r) == /\ state[s] = Candidate
                 /\ Select("accept", r) = {}
                 /\ LET S == Select("propose", r)
                    IN /\ \E Q \in Classic: Cardinality(S) \geq Cardinality(Q)
                       /\ \/ \E v \in Values:
                               /\ ValueForAccept(S, v)
                               /\ Send([type |-> "accept", round |-> r, value |-> v, replica |-> s])
                          \/ /\ ValueForAccept(S, ANY)
                             /\ AddToFast(r)
                             /\ Send([type |-> "accept", round |-> r, value |-> ANY, replica |-> s])
                 /\ state' = [state EXCEPT ![s] = Coordinator]
                 /\ UNCHANGED <<vote, voteRound>>

Phase2B(s) == \E m \in messages:
                 /\ m.type = "accept"
                 /\ m.round \geq round[s]
                 /\ \E v \in Values:
                    /\ \/ m.value = ANY
                       \/ m.value = v
                    /\ Send([type |-> "accepted", value |-> v, round |-> m.round, replica |-> s])
                    /\ round' = [round EXCEPT ![s] = m.round]
                    /\ vote' = [vote EXCEPT ![s] = v]
                    /\ voteRound = [voteRound EXCEPT ![s] = m.round]
                 /\ UNCHANGED <<state, fastRounds>>


IsCommitted(S, r, v) == \E Q \in Quorums(r):
                            \A repl \in Q:
                               \E m \in S:
                                  /\ m.value = v
                                  /\ m.replica = r

Phase3B(s, r) == /\ state[s] = Coordinator
                 /\ LET S == Select("accepted", r)
                    IN \E v \in Values:
                        IF IsCommitted(S, r, v)
                        THEN
                          /\ Send([type |-> "learned", value |-> v, round |-> r])
                       \* coordinated recovery
                        ELSE
                            \* send safe value or ANY value from Values set
                             /\ \/ ValueForAccept(S, v)
                                \/ ValueForAccept(S, ANY)
                             /\ Send([type |-> "accept", value |-> v, round |-> r + 1])
                 /\ UNCHANGED <<round, state, fastRounds, vote, voteRound>>

Next == \E s \in Servers:
           \/ Phase1B(s)
           \/ Phase2B(s)
           \/ RenounceLeadership(s)
           \/ \E r \in Rounds:
              \/ Phase1A(s, r)
              \/ Phase2A(s, r)
              \/ Phase3B(s, r)

Spec == Init /\ [][Next]_vars

Learned(v) == \E m \in messages:
                /\ m.type = "learned"
                /\ m.value = v

Consistency == \A v1, v2 \in Values: Learned(v1) /\ Learned(v2) => (v1 = v2)
=============================================================================
\* Modification History
\* Last modified Mon May 11 17:24:52 EEST 2020 by dd
\* Created Mon May 11 13:09:43 EEST 2020 by dd
