# Distributed Google File System (GFS) Implementation

A distributed file system implementation inspired by Google's GFS, built using Go, gRPC, and modern distributed systems technologies.

##  Architecture

This project implements a distributed file system with the following components:

- **Master Server**: Central coordinator managing metadata, chunk locations, and file operations
- **Chunk Servers**: Storage nodes handling actual data chunks with replication
- **Shadow Masters**: Backup masters for fault tolerance and automatic failover
- **Client Interface**: RESTful-like interface for file operations

##  Features

- **Distributed Storage**: 64MB chunk-based storage with 3-way replication
- **Fault Tolerance**: Automatic failover through shadow masters
- **High Availability**: Leader election using etcd for distributed coordination
- **File Operations**: Complete CRUD operations (Create, Read, Write, Append, Delete)
- **Directory Support**: Hierarchical directory structure management
- **Monitoring**: System resource tracking and health monitoring

##  Technologies Used

- **Backend**: Go (Golang)
- **Communication**: gRPC with Protocol Buffers
- **Coordination**: etcd for leader election and service discovery
- **Monitoring**: gopsutil for system resource tracking
- **Build System**: Makefile for automation
- **Deployment**: Shell scripts for multi-server orchestration

##  Project Structure

```
gfs/
├── client/                 # Client implementation and test data
│   ├── data/              # Test data files
│   ├── features_queries/  # JSON query definitions
│   └── main.go           # Client main application
├── server/
│   ├── masterServer/      # Master server implementation
│   └── chunkServer/      # Chunk server implementation
├── protofiles/           # Protocol Buffer definitions
│   ├── master/          # Master service definitions
│   └── chunkserver/     # Chunk server service definitions
├── logs/                # System logs and operation logs
├── Makefile            # Build and deployment automation
├── go.mod             # Go module dependencies
└── README.md          # This file
```

##  Quick Start

### Prerequisites

- Go 1.23.5 or later
- Protocol Buffer compiler (protoc)
- etcd server

### Build and Run

1. **Generate Protocol Buffer files**:
   ```bash
   make proto
   ```

2. **Start Master Server**:
   ```bash
   make master-server
   ```

3. **Start Shadow Master** (for fault tolerance):
   ```bash
   make shadow-master
   ```

4. **Start Chunk Servers**:
   ```bash
   make chunk-servers
   ```

5. **Run Client Tests**:
   ```bash
   make client
   ```

### Configuration

- **Number of Chunk Servers**: Set `NUM_CHUNK_SERVERS` environment variable (default: 10)
- **Base Port**: Set `BASE_PORT` environment variable (default: 50050)
- **Shadow Port**: Set `SHADOW_PORT` environment variable (default: 50070)

##  API Operations

The system supports the following file operations:

- **Create File/Directory**: Initialize new files or directories
- **Read**: Retrieve file data with chunk-based reading
- **Write**: Write data to specific file offsets
- **Append**: Append data to the end of files
- **Delete/Undelete**: Soft delete with recovery capability

## System Design

### Master Server
- Manages file system metadata and namespace
- Coordinates chunk placement and replication
- Handles client requests and chunk server heartbeats
- Implements operation logging for consistency

### Chunk Servers
- Store actual file data in 64MB chunks
- Handle read/write operations with version control
- Participate in replication and recovery processes
- Report health status via heartbeats

### Fault Tolerance
- **Shadow Masters**: Backup masters for automatic failover
- **Chunk Replication**: 3-way replication for data durability
- **Leader Election**: etcd-based coordination for master selection
- **Health Monitoring**: Continuous monitoring of server status

##  Performance Characteristics

- **Chunk Size**: 64MB for optimal performance
- **Replication Factor**: 3 replicas per chunk
- **Heartbeat Interval**: 2 seconds for health monitoring
- **RPC Timeout**: 10 seconds for network operations
- **File Deletion Delay**: 5 seconds for soft delete recovery
