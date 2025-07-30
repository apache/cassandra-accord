# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

This is a Gradle-based Java project. Key commands:

- **Build**: `./gradlew build`
- **Test**: `./gradlew test`
- **Install locally**: `./gradlew install` (or `./gradlew publishToMavenLocal`)
- **Burn testing**: `./gradlew burn` (stress testing with 1 cluster)
- **Burn loop**: `./gradlew burnloop` (continuous burn testing, configurable with `-PburnTimes=N`)

## Core Architecture

This is **Apache Cassandra Accord**, a general-purpose transactions library implementing a leaderless consensus protocol for highly available transactions.

### Key Components

1. **Node** (`accord.local.Node`): Central coordination point that manages transaction lifecycle, command stores, and cluster communication.

2. **Command/Transaction Flow**:
   - **Txn** (`accord.primitives.Txn`): Transaction definition with Read/Write/EphemeralRead kinds
   - **Command** (`accord.local.Command`): Local representation of transactions with status tracking
   - **TxnId** (`accord.primitives.TxnId`): Globally unique transaction identifiers

3. **Coordination** (`accord.coordinate.*`):
   - **CoordinateTransaction**: Orchestrates transaction execution across cluster
   - **PreAccept/Accept**: Two-phase consensus protocol messages
   - Various tracking classes for managing quorums and responses

4. **Storage & State**:
   - **CommandStore** (`accord.local.CommandStore`): Per-shard transaction storage
   - **SafeCommandStore**: Thread-safe wrapper for command operations
   - **CommandsForKey** (CFK): Efficient key-based command indexing

5. **Messaging** (`accord.messages.*`): Protocol messages for cluster communication including PreAccept, Accept, Apply, ReadData, etc.

6. **Primitives** (`accord.primitives.*`):
   - **Route/Keys/Ranges**: Data locality and routing
   - **Deps**: Transaction dependency tracking
   - **Status/SaveStatus**: Transaction state management
   - **Ballot**: Consensus voting

7. **Topology** (`accord.topology.*`): Cluster membership and shard management

8. **Utils** (`accord.utils.*`): Specialized data structures including BTree implementations, async utilities, and custom collections

### Architecture Notes

- Uses leaderless consensus (no single coordinator)
- Implements dependency tracking for conflict resolution
- Supports both synchronous and asynchronous operations via AsyncChain
- Built for high availability with configurable durability levels
- Custom serialization and memory-efficient data structures throughout