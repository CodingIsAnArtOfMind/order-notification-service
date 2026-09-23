# Java Concurrency Learning Notes
## Platform Threads, Thread Pools, Virtual Threads, WebFlux, CompletableFuture, CPU vs I/O

These notes document the complete discussion so far, with examples and diagrams.

---

# 1. Core Mental Model

There are four different things that are easy to mix up:

```text
Application Task
      |
      v
Java Thread
      |
      v
Operating System Thread
      |
      v
CPU Core
```

For traditional Java platform threads:

```text
Java Platform Thread ≈ OS / Native Thread
```

For virtual threads:

```text
Virtual Thread
     |
     | temporarily runs on
     v
Carrier Thread
     |
     v
Platform / OS Thread
     |
     v
CPU Core
```

Important:

```text
Virtual Thread != OS Thread
Carrier Thread = Platform Thread
```

However, not every platform thread in the JVM is necessarily a carrier thread.

The JVM can also contain:

```text
GC threads
Kafka threads
Tomcat threads
ForkJoinPool workers
Database pool housekeeping threads
JIT compiler threads
Other framework/library threads
```

---

# 2. CPU Cores vs Threads

Suppose the machine has:

```text
8 CPU cores
```

This means roughly:

```text
Only around 8 threads can literally execute CPU instructions
at the exact same instant.
```

But the operating system can still have:

```text
100 threads
1,000 threads
10,000 threads
```

The number of OS threads is not limited to the number of CPU cores.

The OS scheduler decides which runnable thread gets CPU time.

Example:

```text
100 Platform Threads

P1
P2
P3
...
P100

8 CPU Cores

C1 C2 C3 C4 C5 C6 C7 C8
```

At one instant:

```text
C1 -> P1
C2 -> P2
C3 -> P3
C4 -> P4
C5 -> P5
C6 -> P6
C7 -> P7
C8 -> P8
```

Later:

```text
C1 -> P9
C2 -> P10
C3 -> P11
...
```

The OS switches between runnable threads.

This is called:

```text
Context Switching
Time Slicing
Scheduling
```

---

# 3. Who Creates OS Threads?

The CPU does NOT create threads.

Threads are created by:

```text
Your Application
Framework
JVM
Libraries
```

Example:

```java
ExecutorService executor =
        Executors.newFixedThreadPool(100);
```

Conceptually:

```text
Your Java Code
     |
     v
ThreadPoolExecutor
     |
     v
ThreadFactory
     |
     v
new Thread(...)
     |
     v
JVM
     |
     v
Operating System
     |
     v
OS / Native Thread
```

So:

```text
Application/JVM
    -> decides how many threads should exist

Operating System Scheduler
    -> decides which runnable thread gets CPU

CPU
    -> executes instructions
```

---

# 4. CPU-Bound Work

A CPU-bound task spends most of its time actually computing.

Examples:

```text
Image resizing
Video encoding
Encryption
Hashing
Compression
Large sorting
Large in-memory transformations
Pricing calculations
Risk calculation
Machine learning inference
Matrix multiplication
```

Example:

```java
ExecutorService executor =
        Executors.newFixedThreadPool(100);

for (int i = 0; i < 100; i++) {
    executor.submit(() -> {
        while (true) {
            Math.sqrt(Math.random());
        }
    });
}
```

If the machine has:

```text
8 CPU cores
```

then all 100 threads may want CPU:

```text
P1   -> needs CPU
P2   -> needs CPU
...
P100 -> needs CPU
```

But roughly only 8 can execute at once.

```text
8 threads -> actively executing
92 threads -> runnable, waiting for CPU time
```

Too many threads can make CPU-bound workloads worse because of:

```text
Context switching
Scheduler overhead
CPU cache misses
Memory overhead
```

For CPU-heavy work, thread count is often close to:

```java
Runtime.getRuntime().availableProcessors()
```

For example:

```text
8 cores
~8 CPU workers
```

The exact ideal number depends on workload.

---

# 5. I/O-Bound Work

An I/O-bound task spends much of its time waiting for something external.

Examples:

```text
Database call
REST API call
Redis request
File read/write
Socket read
Network request
Kafka network activity
```

Example:

```java
repository.findById(10L);
```

Possible path:

```text
Java
 |
 v
Spring Data / Hibernate
 |
 v
JDBC Driver
 |
 v
TCP Network
 |
 v
PostgreSQL
```

Suppose the full request takes:

```text
200 ms
```

but actual CPU usage is:

```text
2 ms -> build/send request
198 ms -> wait for DB
```

Timeline:

```text
P1

CPU work          2 ms
    |
    v
Send SQL
    |
    v
-------------------------
WAIT FOR DATABASE 198 ms
-------------------------
    |
    v
CPU work again
    |
    v
Process response
```

During those 198 ms:

```text
The thread exists
but it is not consuming CPU continuously.
```

This is why an 8-core machine can still have 100 or 200 I/O-oriented OS threads.

---

# 6. Simple Waiting Example: Thread.sleep()

This is not network I/O, but it is useful for understanding waiting.

```java
ExecutorService executor =
        Executors.newFixedThreadPool(100);

for (int i = 0; i < 100; i++) {
    executor.submit(() -> {
        try {
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    });
}
```

Now you can have:

```text
100 Platform Threads
```

but CPU usage may still be low:

```text
P1   sleeping
P2   sleeping
P3   sleeping
...
P100 sleeping
```

Therefore:

```text
Number of Threads
!=
Number of CPU Cores
```

---

# 7. Traditional ThreadPoolExecutor

Example:

```java
ExecutorService executor =
        Executors.newFixedThreadPool(100);
```

This can create up to:

```text
100 platform / OS worker threads
```

Suppose 150 tasks are submitted:

```text
Tasks 1-100
    -> can occupy P1-P100

Tasks 101-150
    -> wait in executor queue
```

Important correction:

```text
On an 8-core CPU, it is NOT:

8 threads executing
92 threads parked in ThreadPoolExecutor queue
```

Instead:

```text
100 OS worker threads can exist.

Only ~8 can execute CPU instructions literally at once.

Others may be:
- runnable but waiting for CPU time
- blocked on I/O
- sleeping
- waiting on locks
```

If all 100 workers call the DB:

```text
P1   -> DB wait
P2   -> DB wait
P3   -> DB wait
...
P100 -> DB wait
```

then all 100 worker threads are occupied.

Task 101 cannot start until one worker becomes free.

So for ThreadPoolExecutor:

```text
Concurrency limit is often determined by pool size,
not directly by number of CPU cores.
```

---

# 8. Why Traditional Backend Servers Used Large Thread Pools

Suppose each request takes:

```text
100 ms total
```

but only:

```text
5 ms CPU work
95 ms waiting for DB / APIs
```

If we only had 8 threads:

```text
8 requests start
all 8 wait for I/O
new requests queue
```

Even though the CPU may be mostly idle.

So traditional server applications often used:

```text
100
200
300
```

platform threads.

Example Tomcat setting:

```properties
server.tomcat.threads.max=200
```

This allows many requests to be waiting on external I/O simultaneously.

However platform threads are relatively expensive:

```text
OS scheduling
Native stack
Memory
Context switching
Kernel resources
```

That is one reason virtual threads are valuable.

---

# 9. Traditional Blocking Model

Traditional request handling:

```text
Request
   |
   v
Platform Thread P1
   |
   v
DB Call
   |
   v
WAIT
   |
   v
P1 remains occupied
```

If the DB call takes 500 ms:

```text
P1 waits for 500 ms
```

The CPU may not be doing work for P1, but that OS thread is still tied to that request.

---

# 10. CompletableFuture

Example:

```java
CompletableFuture.supplyAsync(() ->
        repository.findById(10L)
);
```

Without a custom executor, `supplyAsync()` generally uses:

```text
ForkJoinPool.commonPool()
```

Conceptually:

```text
Request/Main Thread
        |
        | submit
        v
Worker Platform Thread P4
        |
        v
repository.findById(...)
        |
        v
DB wait
```

Result:

```text
Original request thread may be free
but worker platform thread P4 is blocked/waiting.
```

So:

```text
CompletableFuture
    != automatic non-blocking I/O
```

It often means:

```text
Move blocking work to another thread.
```

Mental model:

```text
CompletableFuture
    -> delegate work to worker thread

If worker performs blocking I/O:
    -> worker platform thread still waits
```

You can combine CompletableFuture with a virtual-thread executor, but that is a separate design.

---

# 11. WebFlux

WebFlux uses a fundamentally different model.

It does NOT normally create a virtual thread for every request.

WebFlux is based on:

```text
Non-blocking I/O
Event loops
Reactive pipelines
Callbacks / continuations
```

Example:

```java
return webClient.get()
        .uri("/price")
        .retrieve()
        .bodyToMono(Price.class);
```

Conceptually:

```text
Event Loop E1
    |
    v
Handle Request A
    |
    v
Start Network Request
    |
    v
Register interest/callback
    |
    v
Do NOT wait
    |
    +----> Handle Request B
    |
    +----> Handle Request C
    |
    +----> Handle Request D
```

When response for A arrives:

```text
Network response
      |
      v
Event Loop notified
      |
      v
Continue Request A pipeline
```

Important:

```text
There is not necessarily a dedicated thread
representing Request A while it waits.
```

This is the major difference from Virtual Threads.

---

# 12. WebFlux Requires Non-Blocking Dependencies

To get the full WebFlux benefit, the chain should be non-blocking.

Example ideal stack:

```text
WebFlux
   |
   v
WebClient
   |
   v
R2DBC
   |
   v
Reactive Redis
   |
   v
Reactive messaging APIs
```

Bad example:

```java
@GetMapping("/orders/{id}")
public Mono<Order> getOrder(@PathVariable Long id) {

    Order order = repository.findById(id).orElseThrow(); // blocking JDBC

    return Mono.just(order);
}
```

This is bad because:

```text
A blocking JDBC call can block an event-loop thread.
```

WebFlux works best when:

```text
Don't block event-loop threads.
```

---

# 13. Virtual Threads

Java 21 introduced virtual threads as a production feature.

Example:

```java
Thread.startVirtualThread(() -> {
    repository.findById(10L);
});
```

Important:

```text
A virtual thread does NOT create its own carrier/platform thread.
```

Instead, many virtual threads share a smaller carrier pool.

Example:

```text
Virtual Threads

V1
V2
V3
V4
...
V100000

        |
        | scheduled over
        v

Carrier / Platform Threads

P1 P2 P3 P4 P5 P6 P7 P8
```

On an 8-core system, scheduler parallelism is often around the number of available processors, but this is not a statement that the JVM has only 8 platform threads overall.

---

# 14. Carrier Thread

A carrier thread is:

```text
A platform thread used to execute virtual threads.
```

Therefore:

```text
Carrier Thread = Platform Thread
```

But:

```text
Not every platform thread is necessarily a carrier.
```

Other platform threads may exist for:

```text
GC
Kafka
Tomcat
ForkJoinPool
JIT
JVM internals
Libraries
```

---

# 15. Mounting a Virtual Thread

Suppose:

```text
Virtual Thread V1
Carrier Thread P3
```

The scheduler may do:

```text
V1
 |
 | mount
 v
P3
 |
 v
CPU
```

Now P3 executes V1's Java code.

Example:

```java
Thread.startVirtualThread(() -> {
    System.out.println("Before DB");

    Order order = repository.findById(10L);

    System.out.println("After DB");
});
```

Execution:

```text
V1 created
   |
   v
V1 RUNNABLE
   |
   v
Scheduler chooses P3
   |
   v
V1 mounts on P3
   |
   v
P3 executes V1 code
```

---

# 16. What Happens When V1 Makes a DB Call?

Suppose:

```java
Order order = repository.findById(10L);
```

The database takes 500 ms.

V1 cannot continue to:

```java
System.out.println("After DB");
```

until the DB responds.

So:

```text
V1 must logically wait.
```

This means:

```text
Virtual Thread V1 = waiting
```

But this does NOT necessarily mean:

```text
Carrier P3 must also wait.
```

That is the important virtual-thread improvement.

---

# 17. Virtual Thread Parking and Unmounting

Conceptually:

```text
V1 running on P3
      |
      v
DB call
      |
      v
Need to wait
      |
      v
V1 parks
      |
      v
V1 unmounts from P3
      |
      v
P3 becomes reusable
```

The JVM can preserve virtual-thread execution state.

A simplified mental model:

```text
V1 stack/state
    |
    v
stored as virtual-thread state / stack chunks
    |
    v
Java heap
```

Then:

```text
V1 = WAITING
P3 = free
```

P3 can now execute another virtual thread.

---

# 18. Carrier Reuse

Example:

```text
P3 timeline

V1
 |
 | DB wait
 v
V1 unmounts
 |
 v
P3 free
 |
 +--> V2
 |
 | API wait
 v
V2 unmounts
 |
 +--> V7
 |
 | CPU work
 v
V7 completes
 |
 +--> V20
 |
 | Redis wait
 v
V20 unmounts
```

So one carrier thread can run many different virtual threads over time.

That is the main scalability advantage.

---

# 19. DB Response Arrives

Suppose PostgreSQL returns data for V1.

Conceptually:

```text
PostgreSQL
    |
    v
Network packet
    |
    v
Operating System
    |
    v
JDK/runtime notices data is ready
    |
    v
V1 becomes RUNNABLE
```

Very simplified OS readiness mechanisms include:

```text
Linux     -> epoll
macOS/BSD -> kqueue
Windows   -> IOCP / related mechanisms
```

You normally do not manually manage these when using virtual threads.

The runtime can track something conceptually like:

```text
V1 is waiting for socket X
```

When data arrives:

```text
WAITING -> RUNNABLE
```

---

# 20. Remounting

V1 does not need to resume on the original carrier.

Example:

```text
Initially:

V1 -> P3
```

Later P3 may be busy.

If P7 is free:

```text
V1
 |
 | mount
 v
P7
 |
 v
continue execution
```

So:

```text
V1 can start on P3
wait
unmount
resume later on P7
```

From Java code, it still looks sequential:

```java
Order order = repository.findById(10L);

System.out.println(order);
```

But under the hood:

```text
V1
 |
 v
P3
 |
 v
DB call
 |
 v
park/unmount

... 500 ms ...

DB response
 |
 v
V1 runnable
 |
 v
P7
 |
 v
continue
```

---

# 21. Important Meaning of "Blocking"

When we say:

```java
repository.findById(10L);
```

is blocking, it means:

```text
The logical Java execution cannot continue
until the result is available.
```

With traditional platform threads:

```text
Logical task blocked
Platform thread also occupied
```

With virtual threads:

```text
Logical virtual thread blocked
Carrier/platform thread can often be reused
```

This is the key difference.

---

# 22. Main Thread Confusion

Example:

```java
public static void main(String[] args) {

    Thread.startVirtualThread(() -> {
        repository.findById(10L);
    });

    System.out.println("Hello");
}
```

Here:

```text
Main Platform Thread
       |
       +--> create V1
       |
       +--> continue
       |
       v
print Hello
```

Separately:

```text
V1
 |
 v
Carrier P1
 |
 v
DB Call
```

The main thread is not automatically blocked.

It blocks only if you explicitly wait:

```java
Thread v1 = Thread.startVirtualThread(...);

v1.join();
```

Then:

```text
main thread
    |
    v
v1.join()
    |
    v
wait until V1 completes
```

---

# 23. Spring Boot Request Model with Virtual Threads

Example:

```java
@GetMapping("/orders/{id}")
public Order getOrder(@PathVariable Long id) {
    return repository.findById(id).orElseThrow();
}
```

With virtual-thread request handling:

```text
HTTP Request
     |
     v
Virtual Thread V123
     |
     v
Carrier P3
     |
     v
JDBC
     |
     v
DB
```

If DB waits:

```text
V123 waits
    |
    v
V123 unmounts
    |
    v
P3 reusable
```

When DB returns:

```text
DB result
   |
   v
V123 becomes runnable
   |
   v
mount on P6
   |
   v
return Order
   |
   v
HTTP Response
```

No special "main request thread" must remain blocked.

---

# 24. Java 21 Pinning Problem

Virtual threads were already useful in Java 21.

Normal blocking I/O was a major use case.

However one important issue was:

```text
Virtual Thread
    |
    v
synchronized block
    |
    v
blocking operation
    |
    v
carrier could become pinned
```

Example:

```java
synchronized (lock) {
    repository.findById(id);
}
```

Conceptually in older behavior:

```text
V1
 |
 | synchronized
 v
DB wait
 |
 v

V1 waits
P1 may also remain stuck/pinned
```

If many requests did this:

```text
V1 -> P1 pinned
V2 -> P2 pinned
V3 -> P3 pinned
...
```

then carrier threads could be exhausted.

Java 24 significantly improved this by removing most `synchronized`-monitor-related pinning.

So:

```text
Java 21:
synchronized + blocking I/O
could pin carrier

Java 24+:
monitor-related pinning is greatly reduced
and normal synchronized blocking can usually unmount
```

Important nuance:

```text
Not every possible blocking operation
is guaranteed to unmount.

Some native/foreign calls can still hold/pin a carrier.
```

---

# 25. ThreadPoolExecutor vs Virtual Threads

Traditional:

```java
ExecutorService executor =
        Executors.newFixedThreadPool(100);
```

Model:

```text
Tasks
  |
  v
Executor Queue
  |
  v
100 Platform Worker Threads
  |
  v
Blocking I/O occupies workers
```

If 100 workers are blocked:

```text
Task 101 waits in queue.
```

Virtual thread model:

```java
try (var executor =
         Executors.newVirtualThreadPerTaskExecutor()) {

    for (int i = 0; i < 1000; i++) {
        executor.submit(() ->
                repository.findById(10L));
    }
}
```

Model:

```text
1000 tasks
   |
   v
1000 Virtual Threads
   |
   v
small carrier pool
   |
   v
platform threads
```

When a virtual thread waits:

```text
VT parks/unmounts
carrier reused
```

Virtual threads are generally not pooled merely to limit thread count.

Instead:

```text
one task ~ one virtual thread
```

---

# 26. Why Not Create Infinite Carrier Threads?

Carrier threads are platform/OS threads.

They still have the traditional costs:

```text
OS scheduling
Native stack
Memory
Kernel resources
Context switching
```

So creating:

```text
100,000 carrier threads
```

would bring back the same old scalability problem.

Virtual threads solve this by allowing:

```text
100,000 logical virtual threads
```

to share a much smaller set of carrier threads.

---

# 27. Restaurant Analogy

Think:

```text
Virtual Threads = customer orders
Carrier Threads = chefs
CPU Cores = actual cooking stations / workers that can execute work
```

Suppose:

```text
1000 orders
8 chefs
```

Order V1 says:

```text
Put chicken in oven for 15 minutes.
```

The chef should not stand in front of the oven doing nothing.

Instead:

```text
Chef P1:
start V1
   |
   v
chicken cooking
   |
   v
leave V1 waiting
   |
   +--> work on V2
   |
   +--> work on V3
   |
   +--> work on V4
```

When V1 is ready:

```text
V1 becomes runnable again
```

Another chef may finish it.

This is similar to:

```text
V1 mounted on P1
wait
unmount
later remount on P5
```

---

# 28. WebFlux vs Virtual Threads

This is one of the most important comparisons.

## Virtual Threads

```text
Request
   |
   v
Virtual Thread
   |
   v
Carrier
   |
   v
DB/API call
   |
   v
Virtual Thread waits
   |
   v
Virtual Thread unmounts
   |
   v
Carrier reused
```

There is still a logical thread per request/task.

## WebFlux

```text
Request
   |
   v
Event Loop
   |
   v
Start non-blocking I/O
   |
   v
Register callback / continuation
   |
   v
Event loop immediately reused
```

There is not necessarily a dedicated thread waiting for that request.

Summary:

```text
Virtual Threads
    = many lightweight logical threads

WebFlux
    = few event-loop threads + non-blocking events/callbacks
```

---

# 29. CompletableFuture vs WebFlux vs Virtual Threads

## CompletableFuture

```text
Request Thread
    |
    v
submit async work
    |
    v
Worker Platform Thread
    |
    v
blocking DB
    |
    v
worker waits
```

Meaning:

```text
Move blocking work to another thread.
```

## WebFlux

```text
Event Loop
    |
    v
start non-blocking operation
    |
    v
register callback
    |
    v
event loop continues other work
```

Meaning:

```text
Do not block threads while waiting.
```

## Virtual Threads

```text
Virtual Thread
    |
    v
normal blocking-style DB/API call
    |
    v
VT waits
    |
    v
VT parks/unmounts
    |
    v
carrier reused
```

Meaning:

```text
Blocking-style programming,
but waiting logical threads are cheap.
```

---

# 30. One-Line Mental Model

Remember:

```text
CompletableFuture
    -> move work to another thread

WebFlux
    -> don't block threads while waiting

Virtual Threads
    -> allow blocking programming style,
       but make waiting threads lightweight
```

---

# 31. CPU-Heavy Work with Virtual Threads

Virtual threads do NOT create more CPU power.

Suppose:

```text
8 CPU cores
50,000 virtual threads
```

and every virtual thread runs:

```java
while (true) {
    calculateSomethingExpensive();
}
```

Then:

```text
50,000 virtual threads
        |
        v
small carrier pool
        |
        v
8 CPU cores
```

Only around 8 computations can truly execute simultaneously.

Virtual threads are most useful for workloads like:

```text
CPU work
   |
   v
wait for DB
   |
   v
CPU work
   |
   v
wait for API
   |
   v
CPU work
```

That describes many backend microservices.

---

# 32. Virtual Threads Do Not Mean Unlimited DB Calls

Suppose:

```text
50,000 Virtual Threads
```

all execute:

```java
repository.findById(id);
```

But HikariCP has:

```properties
maximum-pool-size=20
```

Then only:

```text
20 DB connections
```

exist.

So:

```text
50,000 virtual threads

        |
        v

20 threads can hold/use DB connections

49,980 may wait for a connection
```

Virtual threads make waiting cheaper.

They do NOT remove external resource limits.

Still important:

```text
DB connection pool
Redis connection limits
Kafka limits
Downstream API rate limits
CPU
Memory
Backpressure
Timeouts
Bulkheads
Concurrency limits
```

Therefore:

```text
Virtual Threads != Unlimited Resources
```

---

# 33. Traditional Thread Pool vs Virtual Thread Pool

Traditional:

```text
100 tasks
   |
   v
100 Platform Threads
   |
   v
DB waiting
   |
   v
100 platform threads occupied
```

Virtual:

```text
100,000 tasks
    |
    v
100,000 Virtual Threads
    |
    v
small carrier pool
    |
    v
VT waits -> unmount
    |
    v
carrier reused
```

---

# 34. ThreadPoolExecutor Queue Clarification

Suppose:

```java
Executors.newFixedThreadPool(100);
```

and 100 tasks are submitted.

On an 8-core machine:

```text
Wrong mental model:

8 tasks executing
92 tasks sitting in executor queue
```

Correct mental model:

```text
100 worker platform threads may exist.

Only about 8 can literally execute CPU instructions
at the exact same instant.

Others may be:
- waiting for CPU scheduling
- waiting on DB/network
- sleeping
- blocked on locks
```

The executor queue becomes relevant when more tasks are submitted than available worker capacity.

For example:

```text
Pool size = 100
Tasks submitted = 150

Tasks 1-100:
can be assigned to workers

Tasks 101-150:
wait in executor queue
```

---

# 35. Real Java Examples: CPU vs I/O

## CPU-heavy examples

### Hashing

```java
MessageDigest digest = MessageDigest.getInstance("SHA-256");
byte[] result = digest.digest(largeData);
```

### Compression

```java
try (GZIPOutputStream gzip = new GZIPOutputStream(outputStream)) {
    gzip.write(largeData);
}
```

### Sorting

```java
employees.stream()
        .sorted(Comparator.comparing(Employee::getSalary))
        .toList();
```

### Pricing logic

```java
BigDecimal calculatePrice(Order order) {
    // many calculations
}
```

### Image processing

```java
resizeImage();
applyFilter();
```

These want CPU continuously.

---

## I/O-heavy examples

### JDBC

```java
repository.findById(id);
```

### Redis

```java
redisTemplate.opsForValue().get("order:" + id);
```

### Blocking REST call

```java
restClient.get()
        .uri("/pricing")
        .retrieve()
        .body(Price.class);
```

### File read

```java
Files.readAllBytes(path);
```

### Socket read

```java
socket.getInputStream().read();
```

These often spend significant time waiting for external systems.

---

# 36. Why Virtual Threads Are Attractive for Spring MVC

Traditional Spring MVC:

```text
Request
   |
   v
Tomcat Platform Thread
   |
   v
JDBC / REST
   |
   v
thread waits
```

Benefits:

```text
Simple imperative programming
Easy debugging
Normal stack traces
Simple transaction flow
```

Problem:

```text
Platform thread scalability
```

Virtual-thread-based Spring MVC:

```text
Request
   |
   v
Virtual Thread
   |
   v
normal JDBC / REST code
   |
   v
VT waits and can unmount
```

Benefits:

```text
Simple imperative code
High I/O concurrency
Less reactive complexity
```

This makes the comparison:

```text
Spring MVC + Virtual Threads
vs
Spring WebFlux
```

an important modern backend architecture topic.

---

# 37. WebFlux vs MVC + Virtual Threads

## WebFlux

Good fit when:

```text
You already have reactive stack
End-to-end non-blocking dependencies
Very high concurrency
Streaming/event pipelines
Reactive backpressure is important
```

Programming model:

```text
Mono
Flux
map
flatMap
zip
subscribeOn
publishOn
```

## MVC + Virtual Threads

Good fit when:

```text
Mostly blocking libraries
JDBC
Standard imperative services
Simple request/response flow
Team prefers straightforward code
```

Programming style:

```java
Order order = repository.findById(id).orElseThrow();
Price price = pricingClient.getPrice(id);
return buildResponse(order, price);
```

---

# 38. Important Limitation: Virtual Threads Do Not Make Blocking "Disappear"

Correct statement:

```text
Virtual Thread can still be blocked logically.
```

It simply becomes cheap for the JVM to keep many waiting logical threads.

So:

```text
V1 blocked ✅
Carrier blocked ❌ usually
```

Do NOT think:

```text
Virtual Thread = non-blocking
```

Instead think:

```text
Virtual Thread = blocking-style code with lightweight waiting
```

---

# 39. Final Comparison Table

| Model | Request/Task Representation | What Waits During Blocking I/O? | Thread Cost | Programming Style |
|---|---|---|---|---|
| Traditional Thread Pool | Platform thread | Platform thread | High | Imperative |
| CompletableFuture + normal pool | Worker platform thread | Worker platform thread | High | Async/future-based |
| WebFlux | Reactive pipeline / continuation | Ideally no thread | Low thread count | Reactive |
| Virtual Threads | Virtual thread | Virtual thread, carrier usually reusable | Low per logical task | Imperative |

---

# 40. Final Architecture Diagrams

## Traditional Thread Pool

```text
Tasks
  |
  v
Executor Queue
  |
  v
P1 P2 P3 ... P100
  |
  v
DB/API
  |
  v
Platform workers wait
```

---

## CompletableFuture

```text
Request Thread
     |
     v
CompletableFuture.supplyAsync(...)
     |
     v
Worker Platform Thread
     |
     v
DB/API
     |
     v
Worker waits
```

---

## WebFlux

```text
Request A
   |
   v
Event Loop E1
   |
   v
Start non-blocking I/O
   |
   v
Register continuation
   |
   +----> Request B
   |
   +----> Request C
   |
   +----> Request D

Response A arrives
   |
   v
Event Loop
   |
   v
Continue A
```

---

## Virtual Threads

```text
V1
 |
 | mount
 v
P1
 |
 v
DB call
 |
 v
V1 waits
 |
 v
V1 parks/unmounts

P1
 |
 +--> V2
 |
 +--> V3
 |
 +--> V4


DB response for V1
 |
 v
V1 RUNNABLE
 |
 | mount
 v
P6
 |
 v
continue
```

---

# 41. Full End-to-End Virtual Thread Flow

```text
1. Request arrives

HTTP Request
    |
    v
Virtual Thread V1 created


2. V1 becomes runnable

V1
 |
 v
Scheduler


3. Scheduler chooses a carrier

V1
 |
 | mount
 v
P3
 |
 v
CPU


4. Java code executes

P3 executing V1
 |
 v
repository.findById(10L)


5. JDBC sends request

V1/P3
 |
 v
Hibernate
 |
 v
JDBC Driver
 |
 v
TCP
 |
 v
PostgreSQL


6. DB is slow

V1 cannot continue


7. V1 parks

V1
 |
 v
WAITING


8. V1 unmounts

V1 waiting

P3 FREE


9. P3 runs other VTs

P3 -> V2
P3 -> V7
P3 -> V25


10. PostgreSQL responds

PostgreSQL
 |
 v
OS Network Stack
 |
 v
JDK runtime
 |
 v
V1 becomes RUNNABLE


11. Scheduler chooses any free carrier

V1
 |
 | mount
 v
P6


12. V1 continues

Order order = ...
 |
 v
System.out.println(order)
 |
 v
HTTP Response
```

---

# 42. Most Important Sentences to Remember

```text
1. CPU cores do not create threads.

2. Application/JVM creates threads.

3. OS schedules threads onto CPU cores.

4. 8 cores does not mean only 8 threads can exist.

5. Around 8 CPU-heavy tasks can truly execute simultaneously
   on an 8-core CPU.

6. Hundreds of I/O-oriented OS threads can exist because
   many are waiting rather than consuming CPU.

7. Traditional blocking I/O keeps the platform thread occupied.

8. CompletableFuture often moves blocking work to another
   platform thread; it does not automatically make I/O non-blocking.

9. WebFlux uses event loops and non-blocking I/O.
   It does not normally use one virtual thread per request.

10. Virtual Threads use many lightweight logical threads
    over a much smaller number of carrier/platform threads.

11. When a virtual thread waits on supported blocking I/O,
    it can park and unmount.

12. The carrier can then execute another virtual thread.

13. When the response arrives, the waiting virtual thread
    becomes runnable and can resume on any free carrier.

14. Virtual Threads do not increase CPU power.

15. Virtual Threads do not remove DB pool, API, Redis,
    Kafka, CPU, memory, or rate-limit constraints.

16. Virtual Thread blocking != Carrier Thread blocking.

17. Java 24 significantly improved the synchronized-monitor
    pinning problem that existed with Java 21 virtual threads.
```

---

# 43. Interview-Level Summary

If asked:

> What problem do Virtual Threads solve?

Good answer:

```text
Traditional Java server applications often use one platform thread
per blocking request. For I/O-heavy workloads, those threads spend
most of their lifetime waiting for databases, APIs, Redis, files, etc.

Platform threads are OS threads and are relatively expensive,
so applications traditionally use bounded pools such as 200 Tomcat
workers.

Virtual Threads provide a lightweight Java thread per task/request.
When a virtual thread performs supported blocking I/O and has to wait,
the JVM can park the virtual thread and unmount it from its carrier
platform thread.

The carrier can then execute another virtual thread.

When the I/O completes, the parked virtual thread becomes runnable
again and can resume on any available carrier.

This allows imperative blocking-style code to scale to much higher
I/O concurrency without requiring the reactive programming model.
```

---

# 44. Interview-Level Comparison

If asked:

> WebFlux vs Virtual Threads?

Answer:

```text
WebFlux achieves high concurrency using non-blocking I/O and a small
number of event-loop threads. A request is represented by a reactive
pipeline rather than a dedicated waiting thread.

Virtual Threads keep the traditional thread-per-request programming
model, but each thread is lightweight. When a virtual thread waits on
supported I/O, it can unmount from its carrier platform thread so that
the carrier can execute another virtual thread.

WebFlux avoids blocking threads.

Virtual Threads make blocking-style waiting cheap.
```

---

# 45. Next Topic to Study

The next deeper JVM topic is:

```text
V1
 |
 v
DB call
 |
 v
park
 |
 v
unmount
 |
 v
OS/JDK I/O readiness
 |
 v
response arrives
 |
 v
V1 runnable
 |
 v
remount
```

Things worth studying next:

```text
VirtualThread implementation
ForkJoinPool-based scheduler
Continuation
Stack chunks
LockSupport.park/unpark
Socket I/O integration
Pinning
Structured Concurrency
ScopedValue
Virtual Threads vs CompletableFuture
Virtual Threads vs WebFlux
```

---

# 46. Short Cheat Sheet

```text
8 CPU cores
!=
8 total Java threads


CPU-heavy:
8 cores -> roughly 8 useful simultaneous computations


I/O-heavy:
100+ threads can exist because most may be waiting


Traditional Pool:
Task -> Platform Thread -> DB wait -> Platform Thread occupied


CompletableFuture:
Task -> Worker Platform Thread -> DB wait -> Worker occupied


WebFlux:
Task -> Event Loop -> non-blocking I/O -> event loop reused


Virtual Thread:
Task -> Virtual Thread -> Carrier -> DB wait
                              |
                              v
                    VT parks/unmounts
                              |
                              v
                       Carrier reused
```

---

# 47. Java 24: Pinning Was Improved, Synchronization Was Not Removed

<div align="center">

<h2>Two different problems</h2>

<table>
<tr>
<th>Runtime resource problem</th>
<th>Shared-state correctness problem</th>
</tr>
<tr>
<td><strong>Can the carrier thread be reused?</strong><br>Virtual-thread scheduling and pinning</td>
<td><strong>Can another thread safely read the state?</strong><br>Java Memory Model and synchronization</td>
</tr>
</table>

<br>

<strong>Carrier utilization</strong> &nbsp;≠&nbsp; <strong>lock correctness</strong>

</div>

> **The key idea**
>
> Java 24 significantly improved the virtual-thread carrier pinning problem.
> It did **not** change Java's memory-consistency rules, remove lock contention,
> or make unsynchronized shared-state access safe.

### Follow the flow

```text
1. V1 enters synchronized(lock)
          |
2. V1 writes balance = 900
          |
3. V1 waits for slow DB I/O
          |
4. Java 24 may free the carrier
          |
5. lock is still owned by V1
          |
6. V2 is safe only if it uses the same lock
```

<div align="center">

| Reader style | Can V2 read while V1 owns `lock`? | Visibility guarantee |
|:---:|:---:|:---:|
| No synchronization | ✅ Yes | ❌ None |
| Same `synchronized (lock)` | ❌ No | ✅ Happens-before |
| `volatile` field | ✅ Yes | ✅ Latest volatile write |

</div>

## 47.1 The Example: A Lock Held During Database I/O

```java
private int balance = 1000;
private final Object lock = new Object();

public void update() {
    synchronized (lock) {
        balance = 900;

        slowDbCall(); // V1 waits here

        balance = 920;
    }
}
```

While V1 waits for the database, it still owns `lock`:

<div align="center">

```text
V1
 |
 | synchronized(lock)
 v
balance = 900
 |
 | slowDbCall()
 | DB waiting...
 |
 | lock is STILL owned by V1
```

</div>

Java 24 may allow the virtual thread to unmount from its carrier while it
waits. That improves carrier utilization, but it does not release the
application lock.

## 47.2 An Unsynchronized Reader Has a Data Race

```java
public int getBalance() {
    return balance; // no synchronization
}
```

V2 does not participate in the synchronization protocol. It can therefore
read concurrently with V1, and the program has a **data race**:

<div align="center">

```text
V1                                      V2
 |                                      |
 | synchronized(lock)                   | getBalance() without lock
 |                                      |
 | balance = 900                        +----> can read concurrently ⚠️
 |                                      |
 | slowDbCall()                         |
 |                                      |
 | balance = 920                        |
```

</div>

V2 may observe:

| Possible observation | Meaning |
|---|---|
| `1000` | A stale old value |
| `900` | The intermediate value |
| `920` | The final value |

Without matching synchronization, Java provides no proper visibility guarantee
for this access. **Java 24 did not change that rule.**

## 47.3 A Same-Lock Reader Is Safe

```java
public int getBalance() {
    synchronized (lock) {
        return balance;
    }
}
```

Now V2 must acquire the same monitor:

<div align="center">

```text
V1                                      V2
 |                                      |
 | owns lock                            | attempts lock
 |                                      |      |
 | balance = 900                        |      | waits
 |                                      |      v
 | slowDbCall()                         |   blocked
 |                                      |
 | balance = 920                        |
 | exits synchronized                   |
 | releases lock ---------------------->|
                                        | acquires lock
                                        | reads 920
```

</div>

V2 cannot read `900` from inside its critical section because V1 does not
release the lock until after writing `920`.

### The happens-before guarantee

The Java Memory Model guarantees:

```text
An unlock of a monitor
        happens-before
a later lock of the same monitor
```

Therefore:

```text
V1 writes
   ↓
V1 unlocks
   ↓
V2 locks the same monitor
   ↓
V2 sees V1's completed synchronized writes
```

With proper same-lock synchronization:

| Result | Possible? |
|---|---:|
| Stale value from before the critical section | ❌ |
| Intermediate protected value (`900`) | ❌ |
| Latest completed synchronized state (`920`) | ✅ |

## 47.4 Carrier Thread vs Application Lock

Suppose the database call takes five seconds:

```java
synchronized (lock) {
    balance = 900;

    slowDbCall(); // 5 seconds

    balance = 920;
}
```

The two resources have different states:

<div align="center">

```text
┌──────────────────────┬────────────────────────────┐
│ Carrier thread       │ FREE ✅                    │
├──────────────────────┼────────────────────────────┤
│ Application lock     │ LOCKED for 5 seconds ⚠️    │
└──────────────────────┴────────────────────────────┘
```

</div>

Other callers still queue behind the monitor:

```text
V1 -> waiting for DB, owns lock
V2 -> wants lock, waits
V3 -> wants lock, waits
V4 -> wants lock, waits
V5 -> wants lock, waits
```

Virtual threads make those waiting tasks cheap, but they do not make the
resource concurrent. The access remains serialized.

## 47.5 What Java 24 Did—and Did Not—Fix

| Java 24 improved | Java 24 did not fix |
|---|---|
| Carrier starvation caused by monitor pinning | Lock contention |
| Carrier reuse while a virtual thread waits, where supported | Poor synchronization design |
| Thread utilization for blocking-style workloads | Data races |
| The cost of many waiting virtual threads | Stale reads from unsynchronized access |
|  | Long critical sections |

> **Rule of thumb:** Avoid holding a Java monitor while performing slow
> network or database I/O whenever the business invariant allows it.

## 47.6 Keep the Critical Section Small

Instead of protecting the entire operation:

```java
synchronized (lock) {
    updateState();

    databaseCall(); // slow

    updateStateAgain();
}
```

Consider protecting only the in-memory state transitions:

```java
int value;

synchronized (lock) {
    value = calculateState();
}

Result result = databaseCall(value);

synchronized (lock) {
    applyResult(result);
}
```

The correct design depends on the business invariant. **Do not split every
critical section blindly**: the state may need to remain consistent across
the full operation, in which case a different coordination strategy is
required.

## 47.7 What `volatile` Changes

```java
private volatile int balance;
```

With `volatile`, a write such as:

```java
balance = 900;
```

becomes visible to other threads that read the volatile field. This addresses
ordinary visibility concerns, but it does not make a sequence of operations
transactional.

```text
V1                                      V2
 |                                      |
 | balance = 900                        | reads balance
 | slowDbCall()                         | sees 900
 |                                      |
 | balance = 920                        |
```

V2 can legitimately observe the intermediate value `900` while V1 waits.

<div align="center">

| Mechanism | Visibility | Ordering | Mutual exclusion | Compound-operation atomicity |
|---|---:|---:|---:|---:|
| `synchronized` | ✅ | ✅ | ✅ | Only within the protected section |
| `volatile` | ✅ | ✅ | ❌ | ❌ |

</div>

For example, this is still a compound operation:

```java
balance--;
```

It consists of:

```text
read -> modify -> write
```

`volatile` alone does not make that sequence atomic. Use a lock or an atomic
operation such as `AtomicInteger` when the operation requires atomicity.

## 47.8 Java Memory and Database Concurrency Are Different Layers

Consider a database transaction:

```java
@Transactional
public void updateBalance() {
    Account account =
        repository.findById(1L).orElseThrow();

    account.setBalance(900);

    externalCall();

    account.setBalance(920);
}
```

Another request reading the same database row is not controlled by:

```java
synchronized (lock)
```

That monitor only coordinates threads in the same JVM that use the exact same
lock object. It does not coordinate separate application instances.

Database concurrency depends on a different set of mechanisms:

| Database concern | Typical mechanisms |
|---|---|
| Isolation | Transaction isolation levels |
| Concurrent reads | MVCC |
| Exclusive updates | Row locks |
| Conflict detection | Optimistic locking, `@Version` |
| Explicit serialization | Pessimistic locking, `SELECT ... FOR UPDATE` |
| Scope of consistency | Transaction boundaries |

With multiple Kubernetes pods:

<div align="center">

```text
Pod A: synchronized(lock)
        !=
Pod B: synchronized(lock)
```

</div>

An in-memory Java monitor cannot protect a database record across pods. Use
database-level concurrency control or a deliberate distributed coordination
mechanism.

## 47.9 The Complete Mental Model

<div align="center">

```text
Java 24 virtual-thread improvement
        |
        v
"Can the carrier thread be reused?"
        |
        +---- Usually yes, where the wait can unmount


synchronized semantics
        |
        v
"Can another thread enter the same monitor?"
        |
        +---- No, while the owner holds it


unsynchronized reader
        |
        v
"Can it observe stale/intermediate data?"
        |
        +---- Yes; no matching visibility guarantee


same-lock reader
        |
        v
"Can it read while the writer owns the lock?"
        |
        +---- No

after acquiring the same lock
        |
        +---- Previous synchronized writes are visible
```

</div>

## 47.10 Final Takeaway

<div align="center">

> **Java 24 solved a thread-resource problem, not a shared-data concurrency
> problem.**

<br>

`Carrier utilization` &nbsp;≠&nbsp; `Lock correctness`

</div>

Virtual-thread pinning and shared-state synchronization are orthogonal:

```text
Carrier utilization  !=  Lock correctness
```

Java 24 can make a waiting virtual thread cheaper for the runtime. It cannot
make an unsynchronized read safe, shorten a critical section, remove lock
contention, or coordinate database writes across application instances.
