---
name: mit-65840-ta
description: Teaching Assistant for the MIT 6.5840 Distributed Systems course. Helps with building, testing, and debugging labs (MapReduce, Raft, K/V Server, Sharded K/V).
---

# MIT 6.5840 Teaching Assistant

This skill provides specialized assistance for the MIT 6.5840 Distributed Systems course labs. It helps you navigate the codebase, run tests, and debug common issues in distributed systems.

## Workflow

1.  **Research**: Use this skill to understand the requirements of each lab and the existing codebase.
2.  **Implementation**: Get guidance on implementing Raft, K/V servers, and MapReduce.
3.  **Testing**: Run lab-specific tests using the provided scripts or `Makefile`.
4.  **Debugging**: Analyze test failures and identify potential race conditions or logic errors.

## Lab Navigation

The project is organized into several labs:
- **MapReduce**: Located in `src/mr` and `src/mrapps`.
- **K/V Server**: Located in `src/kvsrv1`.
- **Raft**: Located in `src/raft1`.
- **Replicated K/V**: Located in `src/kvraft1`.
- **Sharded K/V**: Located in `src/shardkv1`.

## Common Commands

### Testing
Use the `Makefile` in the `src` directory to run tests:
- `make mr`: Run MapReduce tests.
- `make kvsrv1`: Run K/V server tests.
- `make raft1`: Run Raft tests.
- `make kvraft1`: Run Replicated K/V tests.
- `make shardkv`: Run Sharded K/V tests.

### Building
- `make all`: Build all lab binaries.
- `make mr-build`: Build MapReduce binaries and plugins.

## Guidance and Troubleshooting

- **Race Conditions**: Always run tests with the `-race` flag (default in Makefile).
- **Deadlocks**: Watch for inconsistent lock ordering.
- **Raft Pitfalls**: Pay close attention to figure 2 in the Raft paper. Ensure term updates and log consistency checks are handled correctly.
- **Persistence**: Verify that state is correctly persisted and restored.

## Resources

- [Lab Overview](references/lab-overview.md): Detailed description of each lab's goals and structure.
- [Test Runner](scripts/run-tests.sh): A script to run tests with common options.
