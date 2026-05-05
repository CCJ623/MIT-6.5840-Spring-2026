# MIT 6.5840 Lab Overview

## Lab 1: MapReduce
**Goal**: Build a MapReduce system that consists of a coordinator and multiple workers. The system should handle worker failures.
- **Coordinator**: Orchestrates tasks (Map and Reduce), handles timeouts, and tracks task completion.
- **Worker**: Requests tasks from the coordinator, executes them, and writes results to intermediate/final files.
- **Key Challenge**: Handling worker crashes and ensuring "exactly-once" semantics (or equivalent) for task execution.

## Lab 2: Key/Value Server
**Goal**: Implement a basic Key/Value server that supports `Get`, `Put`, and `Append` operations.
- **kvsrv1**: A single-machine K/V server.
- **Key Challenge**: Ensuring linearizability in the face of network delays and duplicate RPCs.

## Lab 3: Raft
**Goal**: Implement the Raft consensus algorithm for state machine replication.
- **Part A**: Leader election and heartbeats.
- **Part B**: Log replication.
- **Part C**: Persistence (recovering state after a crash).
- **Part D**: Log compaction (installing snapshots).
- **Key Challenge**: Correctly implementing the state machine transitions, handling term changes, and ensuring log consistency.

## Lab 4: Replicated K/V (kvraft)
**Goal**: Build a fault-tolerant K/V service using Raft.
- **Service**: Built on top of the Raft library from Lab 3.
- **Key Challenge**: Integrating the K/V service with Raft, handling duplicate requests, and ensuring linearizability across multiple servers.

## Lab 5: Sharded K/V
**Goal**: Implement a sharded K/V service where keys are partitioned across multiple replica groups.
- **Shard Controller**: Manages the assignment of shards to groups.
- **Shard Server**: A replicated K/V group (using Raft) that handles a subset of shards.
- **Key Challenge**: Moving shards between groups (reconfiguration) while maintaining service availability and consistency.
