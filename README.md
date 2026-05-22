Reliable Broadcast algorithm with Jepsen Maelstrom
- implement algorithm 3.2 on page 78 of the book using Jepsen Maelstrom https://github.com/jepsen-io/maelstrom
- Crash-stop nodes (fail-stop model)
- Process architecture
  - have a global event queue
  - one thread (main?) reading from standard input messages and adding to the event queue
  - one thread (event processor) takes events from the queue one by one and handles them
  - no events are processed concurrently
  - a timer that puts a timeout event in the queue for PFD
  - a message is an event carrier between processes
  - events are nestable

```
- Abstraction stack
      APP     rbBcast(7)                               APP                                              app
       |                                                | 
      RB      bebBcast(rbData(self,7))                 RB      rbDeliver(p,7)                           app.rb
     /  \                                             /  \
   PFD   BEB  plSend(q,bebBcast(rbData(self,7)))    PFD  BEB   bebDeliver(p,rbData(self,7))             app.rb.pfd, app.rb.beb
    |     |                                          |    |
    PL    PL  serialize to STDOUT                    PL   PL   plDeliver(p,bebBcast(rbData(self,7)))    app.rb.pfd.pl, app.rb.beb.pl
```    
- PL doesn't need to be implemented as shown in the book. It just reads messages from STDIN
  and writes message sto STDOUT
- the message broadcasted will be a number

### Perfect Failure Detector PFD:
```
Implements:
	PerfectFailureDetector, instance P.

Uses:
	PerfectPointToPointLinks, instance pl.

upon event 〈 P, Init 〉 do
	alive := Π;
	detected := ∅;
    starttimer(Δ);

upon event 〈 Timeout 〉 do
	forall p ∈ Π do
		if (p NOT ∈ alive) ∧ (p NOT ∈ detected) then
			detected := detected ∪ {p};
			trigger 〈 P, Crash | p 〉;
		
        trigger 〈 pl, Send | p, [HEARTBEATREQUEST] 〉;
		
    alive := ∅;
    starttimer(Δ);

upon event 〈 pl, Deliver | q, [HEARTBEATREQUEST] 〉 do
	trigger 〈 pl, Send | q, [HEARTBEATREPLY] 〉;

upon event 〈 pl, Deliver | p, [HEARTBEATREPLY] 〉 do
	alive := alive ∪ {p};
```

### Best Effort Broadcast BEB:
```
Implements:
	BestEffortBroadcast, instance beb.

Uses:
	PerfectPointToPointLinks, instance pl.

upon event 〈 beb, Broadcast | m 〉 do
	forall q ∈ Π do
	    trigger 〈 pl, Send | q, m 〉;

upon event 〈 pl, Deliver | p, m 〉 do
	trigger 〈 beb, Deliver | p, m 〉
```

### Lazy Reliable Broadcast
Algorithm 3.2 is the Lazy Reliable Broadcast from the book 'Introduction to Reliable and Secure Distributed Programming', with the following specification:

```
Implements:
ReliableBroadcast, instance rb.

Uses:
	BestEffortBroadcast, instance beb;
	PerfectFailureDetector, instance P.

upon event 〈 rb, Init 〉 do
	correct := Π;
	from[p] := [∅]; # for all p

upon event 〈 rb, Broadcast | m 〉 do
	trigger 〈 beb, Broadcast | [DATA, self, m] 〉;

upon event 〈 beb, Deliver | p, [DATA, s, m] 〉 do
	if m NOT ∈ from[s] then
		trigger 〈 rb, Deliver | s, m 〉;
		from[s] := from[s] ∪ {m};
		if s NOT ∈ correct then
			trigger 〈 beb, Broadcast | [DATA, s, m] 〉;

upon event 〈 P, Crash | p 〉 do
	correct := correct \ {p};
	forall m ∈ from[p] do
		trigger 〈 beb, Broadcast | [DATA, p, m] 〉
```
