package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"os/signal"
    "syscall"
	
	clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
	"github.com/google/uuid"
	chunkserverpb "github.com/laharisiri123/Distributed-Google-File-System-Project/protofiles/chunkserver"
	masterpb "github.com/laharisiri123/Distributed-Google-File-System-Project/protofiles/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	basePort          string
	numChunkServers   int
	isShadowMaster    bool
	primaryMasterAddr string
)

const (
	REPLICA_COUNT                             = 3
	RPC_TIMEOUT_INTERVAL                      = time.Second * 10
	CHUNK_SIZE                                = 64 * 1000000
	HEARTBEAT_FAILOVER_INTERVAL               = time.Second * 3 // Consider chunk server dead after this time of no heartbeat
	CHECK_INTERVAL                            = time.Second * 2
	FILE_DELETION_INTERVAL                    = time.Second * 5 // Delete requested files will be deleted if this period is exceeded
	LOG_REPLICATION_INTERVAL                  = 10
	OpCreateFile                OperationType = "CREATE_FILE"
	OpCreateDirectory           OperationType = "CREATE_DIRECTORY"
	OpDeleteFile                OperationType = "DELETE_FILE"
	OpAppendFile                OperationType = "APPEND_FILE"
	OpReadFile                  OperationType = "READ_FILE"
	OpWriteFile                 OperationType = "WRITE_FILE"
	OpCreateChunk               OperationType = "CREATE_CHUNK"
	OpUpdateVersion             OperationType = "UPDATE_CHUNK_VERSION"
	OpGrantLease                OperationType = "GRANT_LEASE"
)

type MasterServer struct {
	address             string
	namespace           *sync.Map                // path -> *FileMetadata
	namespaceLocks      map[string]*sync.RWMutex // locks for namespace management
	namespaceLocksMutex sync.Mutex               //protects the locks map
	chunkMetadata       *sync.Map                // chunkId -> *ChunkMeta
	chunkServers        *sync.Map                // chunkServerAddress -> *ChunkServerState
	reReplicationQueue  *sync.Map                // chunkId -> Target REPLICA_COUNT
	deleteFilesQueue    *sync.Map                // filename -> deletion request time stamp
	opLog               *OperationLog
	shadowMasters       map[string]*ShadowMasterInfo
	shadowMastersMu     sync.Mutex
	isPrimary           bool
	wasOriginalShadow   bool
	etcdClient     		*clientv3.Client
    etcdSession    		*concurrency.Session
    electionKey    		string
    primaryKey     		string
    secondaryKey   		string
    electionTTL    		int
    isShadowLeader 		bool
	masterpb.UnimplementedMasterServiceServer
}

type FileMetadata struct {
	isDir    bool
	chunks   []string  // chunkIds for files
	children *sync.Map // for directories: name -> bool
	created  time.Time
	modified time.Time
	mutex    sync.RWMutex
}

type ChunkMeta struct {
	version     int64
	replicas    []string // chunkServer addresses holding this chunk
	primary     string
	leaseExpiry time.Time    // Lease expiration time
	mutex       sync.RWMutex // per-chunk lock
}

type ChunkServerState struct {
	address       string
	status        string
	chunks        map[string]int64 // chunkId -> version
	diskSpace     uint64
	lastHeartBeat time.Time
	conn          *grpc.ClientConn
	client        chunkserverpb.ChunkserverServiceClient
	mutex         sync.RWMutex
}

type ShadowMasterInfo struct {
	address       string
	conn          *grpc.ClientConn
	client        masterpb.MasterServiceClient
	lastLogSeqNum int64
	isActive      bool
	mu            sync.Mutex
}

type OperationType string

type OperationLog struct {
	logFile *os.File
	logPath string
	mu      sync.Mutex
	seqNum  int64
}

type LogEntry struct {
	Timestamp   time.Time              `json:"timestamp"`
	SequenceNum int64                  `json:"sequence_num"`
	OpType      OperationType          `json:"op_type"`
	Path        string                 `json:"path"`
	Data        map[string]interface{} `json:"data,omitempty"`
}

func NewOperationLog(logDir string) (*OperationLog, error) {
	// Get absolute path
	absPath, err := filepath.Abs(logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %v", err)
	}

	fmt.Printf("Creating operation log in directory: %s\n", absPath)

	if err := os.MkdirAll(absPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %v", err)
	}

	logPath := filepath.Join(absPath, "oplog.json")
	fmt.Printf("Opening log file: %s\n", logPath)

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}

	return &OperationLog{
		logFile: logFile,
		logPath: logPath,
	}, nil
}

func (ol *OperationLog) LogCreateFile(path string) error {
	entry := LogEntry{
		Timestamp:   time.Now(),
		SequenceNum: ol.nextSeqNum(),
		OpType:      OpCreateFile,
		Path:        path,
	}
	return ol.appendLogEntry(entry)
}

func (ol *OperationLog) LogCreateDirectory(path string) error {
	entry := LogEntry{
		Timestamp:   time.Now(),
		SequenceNum: ol.nextSeqNum(),
		OpType:      OpCreateDirectory,
		Path:        path,
	}
	return ol.appendLogEntry(entry)
}

func (ol *OperationLog) LogDeleteFile(path string) error {
	entry := LogEntry{
		Timestamp:   time.Now(),
		SequenceNum: ol.nextSeqNum(),
		OpType:      OpDeleteFile,
		Path:        path,
	}
	return ol.appendLogEntry(entry)
}

func (ol *OperationLog) LogCreateChunk(filePath string, chunkIndex int64, chunkId string) error {
	entry := LogEntry{
		Timestamp:   time.Now(),
		SequenceNum: ol.nextSeqNum(),
		OpType:      OpCreateChunk,
		Path:        filePath,
		Data: map[string]interface{}{
			"chunkIndex": chunkIndex,
			"chunkId":    chunkId,
		},
	}
	return ol.appendLogEntry(entry)
}

func (ol *OperationLog) LogUpdateChunkVersion(chunkId string, newVersion int64) error {
	entry := LogEntry{
		Timestamp:   time.Now(),
		SequenceNum: ol.nextSeqNum(),
		OpType:      OpUpdateVersion,
		Path:        "", // Not file-specific
		Data: map[string]interface{}{
			"chunkId":    chunkId,
			"newVersion": newVersion,
		},
	}
	return ol.appendLogEntry(entry)
}

func (ol *OperationLog) LogGrantLease(chunkId string, primary string, leaseExpiry time.Time) error {
	entry := LogEntry{
		Timestamp:   time.Now(),
		SequenceNum: ol.nextSeqNum(),
		OpType:      OpGrantLease,
		Path:        "", // Not file-specific
		Data: map[string]interface{}{
			"chunkId":     chunkId,
			"primary":     primary,
			"leaseExpiry": leaseExpiry.Unix(),
		},
	}
	return ol.appendLogEntry(entry)
}

func (ol *OperationLog) appendLogEntry(entry LogEntry) error {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	if _, err := ol.logFile.Write(data); err != nil {
		return err
	}

	return nil
}

// Helper method to get and increment sequence number
func (ol *OperationLog) nextSeqNum() int64 {
	ol.mu.Lock()
	defer ol.mu.Unlock()
	ol.seqNum++
	return ol.seqNum
}

func (ol *OperationLog) GetCurrentSequenceNumber() int64 {
	ol.mu.Lock()
	defer ol.mu.Unlock()
	return ol.seqNum
}

func (ol *OperationLog) appendLog(opType OperationType, path string) error {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	entry := LogEntry{
		Timestamp:   time.Now(),
		SequenceNum: ol.seqNum + 1,
		OpType:      opType,
		Path:        path,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	if _, err := ol.logFile.Write(data); err != nil {
		return err
	}

	ol.seqNum++
	return nil
}

// Get all log entries with sequence number greater than since
func (ol *OperationLog) GetLogEntriesSince(since int64) ([]LogEntry, error) {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	// Reopen the log file for reading
	readFile, err := os.Open(ol.logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file for reading: %v", err)
	}
	defer readFile.Close()

	var entries []LogEntry
	scanner := bufio.NewScanner(readFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var entry LogEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			log.Printf("Warning: skipping malformed log entry: %v", err)
			continue
		}

		if entry.SequenceNum > since {
			entries = append(entries, entry)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading log file: %v", err)
	}

	return entries, nil
}

func NewMasterServer(address string) *MasterServer {
	// Create operation log with absolute path
	logDir := filepath.Join(".", "logs", "master"+address)
	fmt.Printf("Initializing operation log in: %s\n", logDir)

	opLog, err := NewOperationLog(logDir)
	if err != nil {
		log.Fatalf("Failed to initialize operation log: %v", err)
	}
	master := &MasterServer{
		address:            address,
		namespace:          &sync.Map{},
		namespaceLocks:     make(map[string]*sync.RWMutex),
		chunkMetadata:      &sync.Map{},
		chunkServers:       &sync.Map{},
		reReplicationQueue: &sync.Map{},
		deleteFilesQueue:   &sync.Map{},
		opLog:              opLog,
	}

	// Initialize root directory
	root := &FileMetadata{
		isDir:    true,
		children: &sync.Map{},
		created:  time.Now(),
		modified: time.Now(),
	}
	master.namespace.Store("/", root)

	return master
}

func (master *MasterServer) keepPrimaryKeyAlive(leaseID clientv3.LeaseID) {
    keepAliveCh, err := master.etcdClient.KeepAlive(context.Background(), leaseID)
    if err != nil {
        log.Fatalf("Failed to keep lease alive: %v", err)
    }
    
    for {
        select {
        case _, ok := <-keepAliveCh:
            if !ok {
                log.Printf("Keep alive channel closed, attempting to reestablish")
                // Try to reestablish
                ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
                leaseResp, err := master.etcdClient.Grant(ctx, int64(master.electionTTL))
                cancel()
                
                if err != nil {
                    log.Fatalf("Failed to create new lease: %v", err)
                }
                
                ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
                _, err = master.etcdClient.Put(ctx, master.primaryKey, master.address, clientv3.WithLease(leaseResp.ID))
                cancel()
                
                if err != nil {
                    log.Fatalf("Failed to register as primary: %v", err)
                }
                
                keepAliveCh, err = master.etcdClient.KeepAlive(context.Background(), leaseResp.ID)
                if err != nil {
                    log.Fatalf("Failed to keep lease alive: %v", err)
                }
            }
        }
    }
}

func (master *MasterServer) monitorPrimaryWithEtcd() {
    // Watch for changes on the primary key
    watchCh := master.etcdClient.Watch(context.Background(), master.primaryKey, clientv3.WithPrefix())
    
    log.Printf("Shadow master started monitoring primary")
    
    for watchResp := range watchCh {
        for _, event := range watchResp.Events {
            // Primary key deleted - possible primary failure
            if event.Type == clientv3.EventTypeDelete {
                log.Printf("Primary master key deleted, initiating election")
                master.participateInElection()
                break
            }
        }
    }
}

func (master *MasterServer) participateInElection() {
    // Skip if we're already primary or shadow leader
    if master.isPrimary || master.isShadowLeader {
        return
    }
    
    log.Printf("Shadow master participating in election")
    
    // Create an election instance
    election := concurrency.NewElection(master.etcdSession, master.electionKey)
    
    // Try to become the leader
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    // Campaign to become leader
    if err := election.Campaign(ctx, master.address); err != nil {
        log.Printf("Failed to campaign in election: %v", err)
        return
    }
    
    // Check if we won the election
    ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    resp, err := election.Leader(ctx)
    if err != nil {
        log.Printf("Failed to get election leader: %v", err)
        return
    }
    
    // If we are the leader, update primary key
    if string(resp.Kvs[0].Value) == master.address {
        log.Printf("Won election, becoming shadow leader")
        
        ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        
        // Put our address in the primary key
        leaseResp, err := master.etcdClient.Grant(ctx, int64(master.electionTTL))
        if err != nil {
            log.Printf("Failed to create lease: %v", err)
            return
        }
        
        _, err = master.etcdClient.Put(ctx, master.primaryKey, master.address, clientv3.WithLease(leaseResp.ID))
        if err != nil {
            log.Printf("Failed to update primary key: %v", err)
            return
        }
        
        // Keep lease alive
        go master.keepPrimaryKeyAlive(leaseResp.ID)
        
        // Become shadow leader
        master.transitionToShadowLeader()
    }
}

func (master *MasterServer) transitionToShadowLeader() {
    master.shadowMastersMu.Lock()
    defer master.shadowMastersMu.Unlock()
    
    if master.isShadowLeader {
        return
    }
    
    log.Println("Transitioning to shadow leader")
    
    // Update state
    master.isShadowLeader = true
    
    // We're ready to serve read requests
    log.Println("Successfully transitioned to shadow leader (read-only mode)")
}

func (master *MasterServer) allowWriteOperation() error {
    if master.isShadowLeader {
        return status.Error(codes.FailedPrecondition,
            "This master is in read-only mode after failover. Only read operations are allowed.")
    }
    return nil
}

func (master *MasterServer) checkPrimaryReturn() {
    // Only do this if we are a shadow leader
    if !master.isShadowLeader {
        return
    }
    
    // Watch for changes on the primary key
    watchCh := master.etcdClient.Watch(context.Background(), master.primaryKey, clientv3.WithPrefix())
    
    for watchResp := range watchCh {
        for _, event := range watchResp.Events {
            // Primary key created or updated - possible primary return
            if event.Type == clientv3.EventTypePut {
                newPrimary := string(event.Kv.Value)
                if newPrimary != master.address && master.isPrimaryAddressOriginal(newPrimary) {
                    log.Printf("Original primary has returned: %s. Stepping down.", newPrimary)
                    master.stepDownAsShadowLeader()
                    return
                }
            }
        }
    }
}

func (master *MasterServer) stepDownAsShadowLeader() {
    master.shadowMastersMu.Lock()
    defer master.shadowMastersMu.Unlock()
    
    if !master.isShadowLeader {
        return
    }
    
    log.Println("Stepping down as shadow leader")
    
    // Update state
    master.isShadowLeader = false
    
    // Resign from election
    if master.etcdSession != nil {
        election := concurrency.NewElection(master.etcdSession, master.electionKey)
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        
        if err := election.Resign(ctx); err != nil {
            log.Printf("Failed to resign from election: %v", err)
        }
    }
    
    // Remove our address from the primary key if we still hold it
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    resp, err := master.etcdClient.Get(ctx, master.primaryKey)
    if err == nil && len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == master.address {
        _, err = master.etcdClient.Delete(ctx, master.primaryKey)
        if err != nil {
            log.Printf("Failed to remove primary key: %v", err)
        }
    }

	// Clear chunk server states to stop re-replication attempts
    master.chunkServers = &sync.Map{}
    
    // Clear re-replication queue
    master.reReplicationQueue = &sync.Map{}
    
    log.Println("Successfully stepped down as shadow leader")
}

func (master *MasterServer) isPrimaryAddressOriginal(address string) bool {
    // For this implementation, we'll assume the original primary is the one configured at startup
    return address == primaryMasterAddr
}

func (master *MasterServer) recoverStateFromLogs() error {
    // Get absolute path to log file
    absPath, err := filepath.Abs(master.opLog.logPath)
    if err != nil {
        return fmt.Errorf("failed to get absolute path to log file: %v", err)
    }
    
    log.Printf("Recovering master state from log file: %s", absPath)
    
    // Open log file for reading
    file, err := os.Open(master.opLog.logPath)
    if err != nil {
        if os.IsNotExist(err) {
            log.Printf("No log file found, starting with fresh state")
            return nil
        }
        return fmt.Errorf("failed to open log file: %v", err)
    }
    defer file.Close()
    
    // Read and replay each log entry
    scanner := bufio.NewScanner(file)
    entryCount := 0
    maxSeq := int64(0)
    
    for scanner.Scan() {
        line := scanner.Text()
        if line == "" {
            continue
        }
        
        var entry LogEntry
        if err := json.Unmarshal([]byte(line), &entry); err != nil {
            log.Printf("Warning: skipping malformed log entry: %v", err)
            continue
        }
        
        // Apply the log entry to master's state
        if err := master.applyLogEntry(entry); err != nil {
            // Don't fail completely on individual entry errors
            log.Printf("Warning: error applying log entry %d: %v", entry.SequenceNum, err)
        }
        
        entryCount++
        if entry.SequenceNum > maxSeq {
            maxSeq = entry.SequenceNum
        }
    }
    
    if err := scanner.Err(); err != nil {
        return fmt.Errorf("error reading log file: %v", err)
    }
    
    // Update the sequence number
    master.opLog.mu.Lock()
    master.opLog.seqNum = maxSeq
    master.opLog.mu.Unlock()
    
    log.Printf("Recovery complete: applied %d log entries, sequence number is now %d", 
               entryCount, maxSeq)
    
    // Print the recovered namespace for debugging
    master.printAllnamespaces()
    
    return nil
}

// Initialize shadow master functionality
func (master *MasterServer) initializeShadowMasterSupport(isShadow bool) {
	master.shadowMasters = make(map[string]*ShadowMasterInfo)
	master.isPrimary = !isShadow
	master.wasOriginalShadow = isShadow
	master.isShadowLeader = false

	// Initialize etcd
    if err := master.initializeEtcd(); err != nil {
        log.Fatalf("Failed to initialize etcd: %v", err)
    }

	// Register in etcd
    if master.isPrimary {
        // Register as primary
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        
        // Put primary key with lease
        leaseResp, err := master.etcdClient.Grant(ctx, int64(master.electionTTL))
        if err != nil {
            log.Fatalf("Failed to create lease: %v", err)
        }
        
        _, err = master.etcdClient.Put(ctx, master.primaryKey, master.address, clientv3.WithLease(leaseResp.ID))
        if err != nil {
            log.Fatalf("Failed to register as primary: %v", err)
        }
        
        // Keep lease alive
        go master.keepPrimaryKeyAlive(leaseResp.ID)
        
        // Start log replication worker
        go master.logReplicationWorker()
    } else {
        // Register as shadow
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        
        secondaryKey := fmt.Sprintf("%s/%s", master.secondaryKey, master.address)
        _, err := master.etcdClient.Put(ctx, secondaryKey, master.address)
        if err != nil {
            log.Fatalf("Failed to register as shadow: %v", err)
        }
        
        // Start monitoring primary
        go master.monitorPrimaryWithEtcd()
    }
}

// Register a shadow master
func (master *MasterServer) RegisterShadowMaster(address string) error {
	if !master.isPrimary {
		return fmt.Errorf("Only primary master can register shadows.")
	}

	master.shadowMastersMu.Lock()
	defer master.shadowMastersMu.Unlock()

	// Check if already registered
	if _, exists := master.shadowMasters[address]; exists {
		return nil
	}

	// Connect to the shadow master
	conn, err := grpc.Dial("localhost:"+address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to shadow master: %v", err)
	}

	client := masterpb.NewMasterServiceClient(conn)
	master.shadowMasters[address] = &ShadowMasterInfo{
		address:       address,
		conn:          conn,
		client:        client,
		lastLogSeqNum: 0,
		isActive:      true,
	}

	log.Printf("Registered shadow master at %s", address)
	return nil
}

func (master *MasterServer) RegisterShadowMasterRPC(ctx context.Context, req *masterpb.RegisterShadowMasterReq) (*masterpb.RegisterShadowMasterRes, error) {
	if !master.isPrimary {
		return nil, status.Error(codes.FailedPrecondition, "only primary master can register shadows")
	}

	err := master.RegisterShadowMaster(req.Address)
	if err != nil {
		return &masterpb.RegisterShadowMasterRes{Success: false}, err
	}

	// Return current log sequence number
	currentSeq := master.opLog.GetCurrentSequenceNumber()

	return &masterpb.RegisterShadowMasterRes{
		Success:       true,
		CurrentLogSeq: currentSeq,
	}, nil
}

// Worker that periodically replicates logs to shadow masters
func (master *MasterServer) logReplicationWorker() {
	ticker := time.NewTicker(LOG_REPLICATION_INTERVAL * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		master.replicateLogsToShadows()
	}
}

// Replicate logs to all shadow masters
func (master *MasterServer) replicateLogsToShadows() {
	master.shadowMastersMu.Lock()
	activeShadows := make([]*ShadowMasterInfo, 0)
	for _, shadow := range master.shadowMasters {
		if shadow.isActive {
			activeShadows = append(activeShadows, shadow)
		}
	}
	master.shadowMastersMu.Unlock()

	if len(activeShadows) == 0 {
		return
	}

	// For each shadow, find pending logs and send them
	for _, shadow := range activeShadows {
		shadow.mu.Lock()
		lastSeqNum := shadow.lastLogSeqNum
		shadow.mu.Unlock()

		logs, err := master.opLog.GetLogEntriesSince(lastSeqNum)
		if err != nil {
			log.Printf("Error getting logs for shadow %s: %v", shadow.address, err)
			continue
		}

		if len(logs) == 0 {
			continue
		}

		err = master.sendLogsToShadow(shadow, logs)
		if err != nil {
			log.Printf("Failed to send logs to shadow %s: %v", shadow.address, err)
		}
	}
}

// Send logs to a specific shadow master
func (master *MasterServer) sendLogsToShadow(shadow *ShadowMasterInfo, logs []LogEntry) error {
	// Skip if no logs to send
	if len(logs) == 0 {
		return nil
	}

	// Convert logs to proto format
	protoLogs := make([]*masterpb.LogEntryProto, 0, len(logs))
	for _, entry := range logs {
		dataJSON, _ := json.Marshal(entry.Data)
		protoLogs = append(protoLogs, &masterpb.LogEntryProto{
			SequenceNum: entry.SequenceNum,
			OpType:      string(entry.OpType),
			Path:        entry.Path,
			DataJson:    string(dataJSON),
			Timestamp:   entry.Timestamp.Unix(),
		})
	}

	// Send logs to shadow master
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := shadow.client.ReplicateLogRPC(ctx, &masterpb.ReplicateLogReq{
		Entries:      protoLogs,
		IsCheckpoint: false,
	})

	if err != nil {
		return err
	}

	if res.Success {
		shadow.mu.Lock()
		shadow.lastLogSeqNum = res.LastAppliedSeq
		shadow.mu.Unlock()
		log.Printf("Successfully replicated %d logs to shadow %s", len(logs), shadow.address)
	}

	return nil
}

/////////////////// Helper functions ///////////////////////////////////////////////////////

func connectToServer(address string) (*grpc.ClientConn, chunkserverpb.ChunkserverServiceClient, error) {
	conn, err := grpc.NewClient("localhost:"+address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to chunkserver with address %s: %v\n", address, err)
		return nil, nil, err
	}
	client := chunkserverpb.NewChunkserverServiceClient(conn)
	return conn, client, nil
}

func (master *MasterServer) getChunkServerClient(address string) (chunkserverpb.ChunkserverServiceClient, error) {
	chunkServerInterface, exists := master.chunkServers.Load(address)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "Chunkserver %s not found", address)
	}

	chunkServer, ok := chunkServerInterface.(*ChunkServerState)
	if !ok {
		return nil, status.Errorf(codes.Internal, "Invalid chunk server state type for %s", address)
	}

	if chunkServer.conn == nil || chunkServer.client == nil {
		conn, client, err := connectToServer(address)
		if err != nil {
			return nil, err
		}
		chunkServer.mutex.Lock()
		chunkServer.conn = conn
		chunkServer.client = client
		chunkServer.mutex.Unlock()
	}

	return chunkServer.client, nil
}

func (master *MasterServer) getLockOnNamespace(path string) *sync.RWMutex {
	master.namespaceLocksMutex.Lock()
	defer master.namespaceLocksMutex.Unlock()

	if lock, exists := master.namespaceLocks[path]; exists {
		return lock
	}

	lock := &sync.RWMutex{}
	master.namespaceLocks[path] = lock
	return lock
}

func (master *MasterServer) acquireLocksOnNamespace(path string, writeOnLeaf bool) {
	components := strings.Split(path, "/")
	currPath := ""

	// Acquire locks in order from root to leaf

	// fmt.Printf("Acquiring locks for path: %s\n", path)
	rootlock := master.getLockOnNamespace("/")
	rootlock.RLock()
	for i, component := range components {
		if component == "" {
			continue
		}

		currPath += "/" + component
		// fmt.Printf("currpath: %s\n", currPath)
		lock := master.getLockOnNamespace(currPath)

		// Acquire read-locks on all parent parent directories
		if i < len(components)-1 {
			lock.RLock()
		} else {
			// Leaf acquires either read or write lock
			if writeOnLeaf {
				lock.Lock()
			} else {
				lock.RLock()
			}
		}
	}
}

func (master *MasterServer) releaseLocksOnNamespace(path string, writeOnLeaf bool) {
	components := strings.Split(path, "/")
	currPath := path

	// Release locks in revers order from leaf to root

	// fmt.Printf("Releasing locks for path: %s\n", path)
	for i := len(components) - 1; i >= 0; i-- {
		// fmt.Printf("currpath: %s\n", currPath)
		lock := master.getLockOnNamespace(currPath)

		// Relase read-locks on all parent parent directories
		if i < len(components)-1 {
			lock.RUnlock()
		} else {
			// Leas releases either read or write lock
			if writeOnLeaf {
				lock.Unlock()
			} else {
				lock.RUnlock()
			}
		}
		currPath = filepath.Dir(currPath)

	}
}

func (master *MasterServer) getChunkMeta(chunkId string) (*ChunkMeta, error) {
	if chunkId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "ChunkId cannot be empty")
	}

	chunkMetaInterface, exists := master.chunkMetadata.Load(chunkId)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "Chunk with Id: %s is Not found", chunkId)
	}

	chunkMeta, ok := chunkMetaInterface.(*ChunkMeta)
	if !ok {
		return nil, status.Errorf(codes.Internal, "Invalid chunk metadata type for %s", chunkId)
	}

	return chunkMeta, nil
}

func (master *MasterServer) addReplicaToChunk(chunkId string, replicaAddr string) error {
	chunkMetaInterface, exists := master.chunkMetadata.Load(chunkId)
	if !exists {
		// Create a new ChunkMeta if it doesn't exist
		chunkMeta := &ChunkMeta{
			version:  0,
			replicas: make([]string, 0),
		}
		master.chunkMetadata.Store(chunkId, chunkMeta)
		chunkMetaInterface = chunkMeta
	}

	chunkMeta, ok := chunkMetaInterface.(*ChunkMeta)
	if !ok {
		return status.Errorf(codes.Internal, "Invalid chunk metadata type for %s", chunkId)
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	// check if Replica Already exists
	for _, addr := range chunkMeta.replicas {
		if addr == replicaAddr {
			return nil
		}
	}

	chunkMeta.replicas = append(chunkMeta.replicas, replicaAddr)
	fmt.Printf("Added server%s to chunk with id:%s\n", replicaAddr, chunkId)

	return nil
}

func (master *MasterServer) removeReplicaFromChunk(chunkId string, replicaAddr string) error {
	chunkMeta, err := master.getChunkMeta(chunkId)
	if err != nil {
		return err
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	for i, addr := range chunkMeta.replicas {
		if addr == replicaAddr {
			chunkMeta.replicas = append(chunkMeta.replicas[:i], chunkMeta.replicas[i+1:]...)
			fmt.Printf("REMOVED Chunkserver %s from chunk %s\n", replicaAddr, chunkId)
			break
		}
	}
	return nil
}

func (master *MasterServer) addChunkToReplica(replicaAddr string, chunkId string, version int64) error {
	chunkServerInterface, exists := master.chunkServers.Load(replicaAddr)
	if !exists {
		return status.Errorf(codes.NotFound, "Replica %s not found", replicaAddr)
	}

	chunkServer, ok := chunkServerInterface.(*ChunkServerState)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "Invalid chunkserver state type for %s", replicaAddr)
	}

	chunkServer.mutex.Lock()
	defer chunkServer.mutex.Unlock()
	chunkServer.chunks[chunkId] = version
	return nil
}

func (master *MasterServer) printAllnamespaces() {
	fmt.Println("========== FILE NAMESPACE ==========")

	master.namespace.Range(func(key, value interface{}) bool {
		path := key.(string)
		meta, ok := value.(*FileMetadata)
		if !ok {
			fmt.Printf("Invalid metadata for %s\n", path)
			return true
		}

		if meta.isDir {
			fmt.Printf("[DIR] %s\n", path)
			fmt.Printf("       Children:\n")

			meta.children.Range(func(childName, _ interface{}) bool {
				fmt.Printf("         - %s\n", childName.(string))
				return true
			})
		} else {
			fmt.Printf("[FILE] %s\n", path)
			if len(meta.chunks) == 0 {
				fmt.Printf("       No chunks yet\n")
			} else {
				fmt.Printf("       Chunks: %v\n", meta.chunks)
			}
		}
		return true
	})

	fmt.Println("====================================")
}

func (master *MasterServer) fileExists(filePath string) bool {
	fileMetaInterface, exists := master.namespace.Load(filePath)
	if !exists {
		return false
	}

	fileMeta, ok := fileMetaInterface.(*FileMetadata)
	if !ok {
		return false
	}

	return !fileMeta.isDir
}

func (master *MasterServer) getChunkId(filePath string, chunkIndex int64) string {
	fileMetaInterface, exists := master.namespace.Load(filePath)
	if !exists {
		return ""
	}

	fileMeta, ok := fileMetaInterface.(*FileMetadata)
	if !ok {
		return ""
	}

	// check whether chunks length is greater than chunkIndex
	if len(fileMeta.chunks) <= int(chunkIndex) {
		return ""
	}

	return fileMeta.chunks[chunkIndex]
}

func generateChunkId() string {
	return "CHUNKID" + uuid.New().String()
}

// Helper function to get max of two int64s
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Receive and apply replicated logs from the primary master
func (master *MasterServer) ReplicateLogRPC(ctx context.Context, req *masterpb.ReplicateLogReq) (*masterpb.ReplicateLogRes, error) {
	if master.isPrimary {
		return nil, status.Error(codes.FailedPrecondition, "primary master cannot accept replicated logs")
	}

	lastAppliedSeq := int64(0)

	// Get our current log sequence number
	currentSeq := master.opLog.GetCurrentSequenceNumber()
	log.Printf("Current sequence number: %d, received %d log entries", currentSeq, len(req.Entries))

	for _, protoEntry := range req.Entries {
		// Skip entries we've already applied
		if protoEntry.SequenceNum <= currentSeq {
			log.Printf("Skipping already applied entry %d (current seq: %d)",
				protoEntry.SequenceNum, currentSeq)
			lastAppliedSeq = max(lastAppliedSeq, protoEntry.SequenceNum)
			continue
		}

		// Convert proto entry to LogEntry
		var dataMap map[string]interface{}
		if protoEntry.DataJson != "" {
			if err := json.Unmarshal([]byte(protoEntry.DataJson), &dataMap); err != nil {
				log.Printf("Error unmarshaling data JSON: %v", err)
				continue
			}
		}

		entry := LogEntry{
			SequenceNum: protoEntry.SequenceNum,
			OpType:      OperationType(protoEntry.OpType),
			Path:        protoEntry.Path,
			Data:        dataMap,
			Timestamp:   time.Unix(protoEntry.Timestamp, 0),
		}

		// Apply the log entry to shadow master's state
		if err := master.applyLogEntry(entry); err != nil {
			log.Printf("Error applying log entry %d: %v", entry.SequenceNum, err)
			break
		}

		// Write the entry to our local log file
		data, err := json.Marshal(entry)
		if err == nil {
			master.opLog.mu.Lock()
			if _, err := master.opLog.logFile.Write(append(data, '\n')); err != nil {
				log.Printf("Warning: failed to write log entry to local log: %v", err)
			}
			master.opLog.mu.Unlock()
		}

		// Update our opLog's sequence number
		master.opLog.mu.Lock()
		master.opLog.seqNum = max(master.opLog.seqNum, entry.SequenceNum)
		master.opLog.mu.Unlock()
		lastAppliedSeq = entry.SequenceNum
	}

	master.printAllnamespaces()

	return &masterpb.ReplicateLogRes{
		Success:        lastAppliedSeq > 0,
		LastAppliedSeq: lastAppliedSeq,
	}, nil
}

// Apply a single log entry to shadow master's state
func (master *MasterServer) applyLogEntry(entry LogEntry) error {
	log.Printf("Applying log entry: type=%s, seq=%d, path=%s",
		entry.OpType, entry.SequenceNum, entry.Path)

	var err error
	switch entry.OpType {
	case OpCreateFile:
		err = master.applyCreateFile(entry.Path)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			// If the file already exists, this is not an error
			log.Printf("File %s already exists, continuing", entry.Path)
			return nil
		}
		return err

	case OpCreateDirectory:
		err = master.applyCreateDirectory(entry.Path)
		if err != nil && strings.Contains(err.Error(), "already exists") {
			// If the directory already exists, this is not an error
			log.Printf("Directory %s already exists, continuing", entry.Path)
			return nil
		}
		return err

	case OpCreateChunk:
		chunkIndex, _ := entry.Data["chunkIndex"].(float64)
		chunkId, _ := entry.Data["chunkId"].(string)
		return master.applyCreateChunk(entry.Path, int64(chunkIndex), chunkId)

	case OpUpdateVersion:
		chunkId, _ := entry.Data["chunkId"].(string)
		newVersion, _ := entry.Data["newVersion"].(float64)
		return master.applyUpdateChunkVersion(chunkId, int64(newVersion))

	case OpGrantLease:
		chunkId, _ := entry.Data["chunkId"].(string)
		primary, _ := entry.Data["primary"].(string)
		leaseExpiryUnix, _ := entry.Data["leaseExpiry"].(float64)
		leaseExpiry := time.Unix(int64(leaseExpiryUnix), 0)
		return master.applyGrantLease(chunkId, primary, leaseExpiry)

	default:
		return fmt.Errorf("unknown operation type: %s", entry.OpType)
	}
}

// applyCreateFile creates a file in the shadow master's namespace without logging
func (master *MasterServer) applyCreateFile(path string) error {
	if path == "" {
		return fmt.Errorf("file path cannot be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must be absolute")
	}

	master.acquireLocksOnNamespace(path, true) // write lock on new file
	defer master.releaseLocksOnNamespace(path, true)

	// Check if file already exists
	if metaInterface, exists := master.namespace.Load(path); exists {
		meta, ok := metaInterface.(*FileMetadata)
		if !ok {
			return fmt.Errorf("invalid metadata type for %s", path)
		}

		if meta.isDir {
			return fmt.Errorf("%s is a directory", path)
		} else {
			return fmt.Errorf("%s file already exists", path)
		}
	}

	// Check if parent directory exists
	parentDirectory := filepath.Dir(path)
	if parentDirectory == "" {
		parentDirectory = "/"
	}
	parentMetaInterface, exists := master.namespace.Load(parentDirectory)
	if !exists {
		return fmt.Errorf("%s directory does not exist", parentDirectory)
	}

	// Check whether parent is a directory
	parentMeta, ok := parentMetaInterface.(*FileMetadata)
	if !ok {
		return fmt.Errorf("invalid metadata type for %s", parentDirectory)
	}

	if !parentMeta.isDir {
		return fmt.Errorf("%s is not a directory", parentDirectory)
	}

	now := time.Now()
	fileMeta := &FileMetadata{
		isDir:    false,
		chunks:   make([]string, 0),
		created:  now,
		modified: now,
	}

	// Add the new file as a child to parent directory
	parentMeta.mutex.Lock()
	parentMeta.children.Store(filepath.Base(path), true)
	parentMeta.modified = now
	parentMeta.mutex.Unlock()

	// Store the new file in namespace
	master.namespace.Store(path, fileMeta)

	return nil
}

// applyCreateDirectory creates a directory in the shadow master's namespace without logging
func (master *MasterServer) applyCreateDirectory(path string) error {
	if path == "" {
		return fmt.Errorf("directory path cannot be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must be absolute")
	}

	master.acquireLocksOnNamespace(path, true) // write lock on new directory
	defer master.releaseLocksOnNamespace(path, true)

	// Check if directory already exists
	if _, exists := master.namespace.Load(path); exists {
		return fmt.Errorf("%s directory already exists", path)
	}

	// Check if parent directory exists
	parentDirectory := filepath.Dir(path)
	if parentDirectory == "" {
		parentDirectory = "/"
	}

	parentMetaInterface, exists := master.namespace.Load(parentDirectory)
	if !exists {
		return fmt.Errorf("%s directory does not exist", parentDirectory)
	}

	// Check whether parent is a directory
	parentMeta, ok := parentMetaInterface.(*FileMetadata)
	if !ok {
		return fmt.Errorf("invalid metadata type for %s", parentDirectory)
	}

	if !parentMeta.isDir {
		return fmt.Errorf("%s is not a directory", parentDirectory)
	}

	now := time.Now()
	dirMeta := &FileMetadata{
		isDir:    true,
		children: &sync.Map{},
		created:  now,
		modified: now,
	}

	// Add the new directory as a child to parent directory
	parentMeta.mutex.Lock()
	parentMeta.children.Store(filepath.Base(path), true)
	parentMeta.modified = now
	parentMeta.mutex.Unlock()

	// Store the new directory in namespace
	master.namespace.Store(path, dirMeta)

	return nil
}

// applyCreateChunk creates a chunk in the shadow master's metadata without logging
func (master *MasterServer) applyCreateChunk(filePath string, chunkIndex int64, chunkId string) error {
	fileMetaInterface, exists := master.namespace.Load(filePath)
	if !exists {
		return fmt.Errorf("file %s not found", filePath)
	}

	fileMeta, ok := fileMetaInterface.(*FileMetadata)
	if !ok {
		return fmt.Errorf("invalid file metadata type for %s", filePath)
	}

	fileMeta.mutex.Lock()
	defer fileMeta.mutex.Unlock()

	// Check if we need to extend the chunks slice
	currLength := len(fileMeta.chunks)
	if int(chunkIndex) >= currLength {
		// Extend the chunks slice to accommodate the new chunk
		newChunks := make([]string, chunkIndex+1)
		copy(newChunks, fileMeta.chunks)
		fileMeta.chunks = newChunks
	}

	// Store the chunk ID at the specified index
	fileMeta.chunks[chunkIndex] = chunkId

	// Create a new ChunkMeta if it doesn't exist
	_, exists = master.chunkMetadata.Load(chunkId)
	if !exists {
		chunkMeta := &ChunkMeta{
			version:  0,
			replicas: make([]string, 0),
		}
		master.chunkMetadata.Store(chunkId, chunkMeta)
	}

	return nil
}

// applyUpdateChunkVersion updates a chunk's version in the shadow master's metadata without logging
func (master *MasterServer) applyUpdateChunkVersion(chunkId string, newVersion int64) error {
	chunkMetaInterface, exists := master.chunkMetadata.Load(chunkId)
	if !exists {
		return fmt.Errorf("chunk %s not found", chunkId)
	}

	chunkMeta, ok := chunkMetaInterface.(*ChunkMeta)
	if !ok {
		return fmt.Errorf("invalid chunk metadata type for %s", chunkId)
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	chunkMeta.version = newVersion

	return nil
}

// applyGrantLease updates lease information for a chunk in the shadow master's metadata without logging
func (master *MasterServer) applyGrantLease(chunkId string, primary string, leaseExpiry time.Time) error {
	chunkMetaInterface, exists := master.chunkMetadata.Load(chunkId)
	if !exists {
		return fmt.Errorf("chunk %s not found", chunkId)
	}

	chunkMeta, ok := chunkMetaInterface.(*ChunkMeta)
	if !ok {
		return fmt.Errorf("invalid chunk metadata type for %s", chunkId)
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	chunkMeta.primary = primary
	chunkMeta.leaseExpiry = leaseExpiry

	return nil
}

// Handle notification that the primary master has failed
func (master *MasterServer) NotifyMasterFailureRPC(ctx context.Context, req *masterpb.NotifyMasterFailureReq) (*masterpb.NotifyMasterFailureRes, error) {
	if master.isPrimary {
		return &masterpb.NotifyMasterFailureRes{Acknowledged: false},
			status.Error(codes.FailedPrecondition, "Primary master cannot handle failure notifications")
	}

	log.Printf("Received notification that primary master %s has failed", req.FailedMasterAddress)

	// Transition this shadow master to become the primary
	if err := master.transitionToPrimary(); err != nil {
		log.Printf("Failed to transition to primary: %v", err)
		return &masterpb.NotifyMasterFailureRes{Acknowledged: false}, err
	}

	return &masterpb.NotifyMasterFailureRes{Acknowledged: true}, nil
}

func (master *MasterServer) monitorPrimaryMaster() {
	if master.isPrimary {
		return // Only shadow masters monitor the primary
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	consecutiveFailures := 0
	maxConsecutiveFailures := 3 // Number of failures before declaring primary dead

	for range ticker.C {
		// Skip if we're already primary
		if master.isPrimary {
			return
		}

		// Try to contact the primary
		conn, err := grpc.Dial("localhost:"+primaryMasterAddr, grpc.WithInsecure())
		if err != nil {
			consecutiveFailures++
			log.Printf("Failed to connect to primary master (attempt %d/%d): %v",
				consecutiveFailures, maxConsecutiveFailures, err)
		} else {
			client := masterpb.NewMasterServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			// Use a simple heartbeat or ping method
			_, err := client.HeartbeatRPC(ctx, &masterpb.HeartbeatReq{Address: master.address})
			cancel()

			if err != nil {
				consecutiveFailures++
				log.Printf("Primary master heartbeat failed (attempt %d/%d): %v",
					consecutiveFailures, maxConsecutiveFailures, err)
			} else {
				// Reset counter on successful contact
				consecutiveFailures = 0
			}
			conn.Close()
		}

		// If we've had too many consecutive failures, assume primary is down
		if consecutiveFailures >= maxConsecutiveFailures {
			log.Printf("Primary master appears to be down after %d consecutive failures. Initiating takeover...",
				consecutiveFailures)
			err := master.transitionToPrimary()
			if err != nil {
				log.Printf("Failed to transition to primary: %v", err)
			} else {
				log.Printf("Successfully transitioned to primary master")
				return // Exit the monitor loop
			}
		}
	}
}

// Transition this shadow master to become the primary
func (master *MasterServer) transitionToPrimary() error {
	master.shadowMastersMu.Lock()
	defer master.shadowMastersMu.Unlock()

	if master.isPrimary {
		return fmt.Errorf("Already primary")
	}

	log.Println("Transitioning from shadow to primary master")

	// Change role to primary
	master.isPrimary = true

	// Start the log replication worker
	go master.logReplicationWorker()

	// We're ready to serve as the primary
	log.Println("Successfully transitioned to primary master")

	return nil
}

/////////////////// End of Helper functions ///////////////////////////////////////////////////////

/////////////////// Rerepliation functions ///////////////////////////////////////////////////////////

// Periodically check for chunkserver failures and process rereplication
func (master *MasterServer) handleFailuresRereplicationGarbageCollection() {
	ticker := time.NewTicker(CHECK_INTERVAL)
	defer ticker.Stop()

	for range ticker.C {
		master.checkFailedChunkServers()
		master.processReReplicationQueue()
		master.processDeletionQueue()
	}
}

func (master *MasterServer) checkFailedChunkServers() {
	now := time.Now()

	var failedServers []string

	// Identify failed servers
	master.chunkServers.Range(func(key, value interface{}) bool {
		addr := key.(string)
		cs := value.(*ChunkServerState)

		cs.mutex.RLock()
		lastHeartbeat := cs.lastHeartBeat
		cs.mutex.RUnlock()

		if now.Sub(lastHeartbeat) > HEARTBEAT_FAILOVER_INTERVAL {
			failedServers = append(failedServers, addr)
		}
		return true
	})

	// Process each failed server
	for _, addr := range failedServers {
		master.handleChunkServerFailure(addr)
	}
}

func (master *MasterServer) handleChunkServerFailure(addr string) {
	// Mark server as failed
	csInterface, exists := master.chunkServers.Load(addr)
	if !exists {
		return
	}

	cs := csInterface.(*ChunkServerState)
	cs.mutex.Lock()
	cs.status = "FAILED"
	cs.mutex.Unlock()

	// Get all chunks that were on this server
	cs.mutex.RLock()
	chunks := make([]string, 0, len(cs.chunks))
	for chunkId := range cs.chunks {
		chunks = append(chunks, chunkId)
	}
	cs.mutex.RUnlock()

	// For each chunk, remove this server from its replicas and queue for re-replication
	for _, chunkId := range chunks {
		fmt.Printf("Removing chunkserver %s from chunk %s due to chunkserver failure\n", addr, chunkId)
		master.removeReplicaFromChunk(chunkId, addr)
		master.queueChunkForReReplication(chunkId, REPLICA_COUNT)
	}
}

func (master *MasterServer) queueChunkForReReplication(chunkId string, targetReplicaCount int64) {
	// Only update if this chunk isn't in queue or needs more replicas
	if existingCount, loaded := master.reReplicationQueue.Load(chunkId); !loaded ||
		existingCount.(int64) < targetReplicaCount {
		master.reReplicationQueue.Store(chunkId, targetReplicaCount)
	}
}

func (master *MasterServer) processReReplicationQueue() {
	// Create a slice to hold all chunks that need re-replication
	var chunksToReplicate []struct {
		chunkId      string
		targetCount  int64
		currentCount int
	}

	// First pass: collect all chunks and their current replica counts
	master.reReplicationQueue.Range(func(key, value interface{}) bool {
		chunkId := key.(string)
		targetCount := value.(int64)

		// Get current replica count
		chunkMeta, err := master.getChunkMeta(chunkId)
		if err != nil {
			// Chunk no longer exists, remove from queue
			master.reReplicationQueue.Delete(chunkId)
			return true
		}

		chunkMeta.mutex.RLock()
		currentCount := len(chunkMeta.replicas)
		chunkMeta.mutex.RUnlock()

		chunksToReplicate = append(chunksToReplicate, struct {
			chunkId      string
			targetCount  int64
			currentCount int
		}{chunkId, targetCount, currentCount})

		return true
	})

	// Sort by priority (chunks with fewest replicas first)
	sort.Slice(chunksToReplicate, func(i, j int) bool {
		// First sort by how under-replicated they are
		iUnder := chunksToReplicate[i].targetCount - int64(chunksToReplicate[i].currentCount)
		jUnder := chunksToReplicate[j].targetCount - int64(chunksToReplicate[j].currentCount)

		if iUnder != jUnder {
			return iUnder > jUnder // Higher under-replication first
		}

		// If equally under-replicated, sort by absolute count
		return chunksToReplicate[i].currentCount < chunksToReplicate[j].currentCount
	})

	// Process chunks in priority order
	for _, chunk := range chunksToReplicate {
		err := master.reReplicateChunk(chunk.chunkId, chunk.targetCount)
		if err != nil {
			log.Printf("Failed to re-replicate chunk %s: %v", chunk.chunkId, err)
			// Keep in queue if failed
			continue
		}

		// Remove from queue if successful
		master.reReplicationQueue.Delete(chunk.chunkId)
	}
}

func (master *MasterServer) reReplicateChunk(chunkId string, targetReplicaCount int64) error {
	chunkMeta, err := master.getChunkMeta(chunkId)
	if err != nil {
		return fmt.Errorf("failed to get chunk metadata: %v", err)
	}

	chunkMeta.mutex.RLock()
	currentReplicas := chunkMeta.replicas
	version := chunkMeta.version
	chunkMeta.mutex.RUnlock()

	if len(currentReplicas) >= int(targetReplicaCount) {
		return nil // Already have enough replicas
	}

	// Calculate how many new replicas we need
	neededReplicas := int(targetReplicaCount) - len(currentReplicas)

	// Find a healthy source replica
	var sourceReplica string
	for _, replica := range currentReplicas {
		csInterface, exists := master.chunkServers.Load(replica)
		if !exists {
			continue
		}

		cs := csInterface.(*ChunkServerState)
		cs.mutex.RLock()
		healthy := cs.status == "ACTIVE"
		cs.mutex.RUnlock()

		if healthy {
			sourceReplica = replica
			break
		}
	}

	if sourceReplica == "" {
		return fmt.Errorf("no healthy source replica available for chunk %s", chunkId)
	}

	// Get target servers, excluding current replicas
	targetServers, err := master.getReplicasForChunk(neededReplicas, currentReplicas)
	if err != nil || len(targetServers) == 0 {
		return fmt.Errorf("failed to select target servers: %v", err)
	}

	// Initiate clone operations
	for _, targetServer := range targetServers {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		targetClient, err := master.getChunkServerClient(targetServer)
		if err != nil {
			log.Printf("Failed to get client for target server %s: %v", targetServer, err)
			continue
		}

		_, err = targetClient.CloneChunkRPC(ctx, &chunkserverpb.CloneChunkReq{
			ChunkId:      chunkId,
			SourceServer: sourceReplica,
			Version:      version,
		})

		if err != nil {
			log.Printf("Failed to clone chunk %s to %s: %v", chunkId, targetServer, err)
			continue
		}

		// Update metadata if clone succeeded
		chunkMeta.mutex.Lock()
		chunkMeta.replicas = append(chunkMeta.replicas, targetServer)
		chunkMeta.mutex.Unlock()

		master.addChunkToReplica(targetServer, chunkId, chunkMeta.version)
	}

	return nil
}

func (master *MasterServer) processDeletionQueue() {
	var filesToDelete []string
	now := time.Now()

	// First pass: collect files that should be deleted
	master.deleteFilesQueue.Range(func(key, value interface{}) bool {
		path := key.(string)
		deletionTime := value.(time.Time)

		if now.Sub(deletionTime) > FILE_DELETION_INTERVAL {
			filesToDelete = append(filesToDelete, path)
		}
		return true
	})

	// Second pass: actually delete the files
	for _, path := range filesToDelete {
		master.performFileDeletion(path)
	}
}

func (master *MasterServer) performFileDeletion(path string) {
	// Get file metadata
	metaInterface, exists := master.namespace.Load(path)
	if !exists {
		master.deleteFilesQueue.Delete(path)
		return
	}

	meta, ok := metaInterface.(*FileMetadata)
	if !ok || meta.isDir {
		master.deleteFilesQueue.Delete(path)
		return
	}

	// Remove from parent directory
	parentPath := filepath.Dir(path)
	if parentPath == "" {
		parentPath = "/"
	}

	parentMetaInterface, exists := master.namespace.Load(parentPath)
	if exists {
		if parentMeta, ok := parentMetaInterface.(*FileMetadata); ok && parentMeta.isDir {
			parentMeta.mutex.Lock()
			parentMeta.children.Delete(filepath.Base(path))
			parentMeta.modified = time.Now()
			parentMeta.mutex.Unlock()
		}
	}

	// Delete all chunks from chunkservers
	meta.mutex.RLock()
	chunks := make([]string, len(meta.chunks))
	copy(chunks, meta.chunks)
	meta.mutex.RUnlock()

	for _, chunkId := range chunks {
		master.deleteChunkFromAllReplicas(chunkId)
		master.chunkMetadata.Delete(chunkId)
	}

	// Remove from namespace and delete queue
	master.namespace.Delete(path)
	master.deleteFilesQueue.Delete(path)
}

func (master *MasterServer) deleteChunkFromAllReplicas(chunkId string) {
	chunkMeta, err := master.getChunkMeta(chunkId)
	if err != nil {
		return // Chunk metadata not found, nothing to delete
	}

	chunkMeta.mutex.RLock()
	replicas := make([]string, len(chunkMeta.replicas))
	copy(replicas, chunkMeta.replicas)
	chunkMeta.mutex.RUnlock()

	var wg sync.WaitGroup
	for _, replicaAddr := range replicas {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			master.deleteChunkFromReplica(chunkId, addr)
		}(replicaAddr)
	}
	wg.Wait()
}

func (master *MasterServer) deleteChunkFromReplica(chunkId string, replicaAddr string) {
	client, err := master.getChunkServerClient(replicaAddr)
	if err != nil {
		log.Printf("Failed to get client for replica %s: %v", replicaAddr, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()

	_, err = client.DeleteChunkRPC(ctx, &chunkserverpb.DeleteChunkReq{
		ChunkID: chunkId,
	})

	if err != nil {
		log.Printf("Failed to delete chunk %s from replica %s: %v", chunkId, replicaAddr, err)
	}
}

/////////////////// END of Rerepliation functions ///////////////////////////////////////////////////////////

////////////////// Append Operation ////////////////////////////////////////////////////////////

func (master *MasterServer) GetChunkDetailsForAppendRPC(ctx context.Context, req *masterpb.AppendReq) (*masterpb.AppendRes, error) {
	// Check if we're in read-only mode
	if err := master.allowWriteOperation(); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Request cannot be nil")
	}

	if req.FilePath == "" {
		return nil, status.Error(codes.InvalidArgument, "File path cannot be empty")
	}

	// Check whether file exists
	if exists := master.fileExists(req.FilePath); !exists {
		return nil, status.Errorf(codes.NotFound, "File %s not found", req.FilePath)
	}

	// Find the last chunk of the file
	fileMetaInterface, _ := master.namespace.Load(req.FilePath)
	fileMeta := fileMetaInterface.(*FileMetadata)

	fileMeta.mutex.RLock()
	chunkCount := len(fileMeta.chunks)
	fileMeta.mutex.RUnlock()

	if chunkCount == 0 {
		// Create the first chunk if the file has no chunks yet
		err := master.createChunk(req.FilePath, 0)
		if err != nil {
			return nil, err
		}
		fileMetaInterface, _ = master.namespace.Load(req.FilePath)
		fileMeta = fileMetaInterface.(*FileMetadata)
		fileMeta.mutex.RLock()
		chunkCount = len(fileMeta.chunks)
		fileMeta.mutex.RUnlock()
	}

	// Get the last chunk's ID
	lastChunkIndex := int64(chunkCount - 1)
	chunkId := master.getChunkId(req.FilePath, lastChunkIndex)

	// Check if the last chunk is near capacity (close to 64MB)
	// If so, create a new chunk
	chunkMeta, err := master.getChunkMeta(chunkId)
	if err != nil {
		return nil, err
	}

	// Check if we need to create a new chunk based on the current chunk size
	chunkMeta.mutex.RLock()
	replicas := chunkMeta.replicas
	fmt.Printf("REPLICAS: %v\n", replicas)
	chunkMeta.mutex.RUnlock()

	// Determine the current size of the chunk by querying its primary replica
	// or the first available replica if there's no current primary
	var currentChunkSize int64 = 0
	var queryReplica string

	chunkMeta.mutex.RLock()
	if chunkMeta.primary != "" && !time.Now().After(chunkMeta.leaseExpiry) {
		queryReplica = chunkMeta.primary
	} else if len(replicas) > 0 {
		queryReplica = replicas[0]
	}
	chunkMeta.mutex.RUnlock()

	if queryReplica != "" {
		// Query the replica for the current chunk size
		client, err := master.getChunkServerClient(queryReplica)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
			defer cancel()

			sizeRes, err := client.GetChunkSizeRPC(ctx, &chunkserverpb.GetChunkSizeReq{
				ChunkId: chunkId,
			})

			if err == nil {
				currentChunkSize = sizeRes.Size
			} else {
				log.Printf("Failed to get chunk size from %s: %v", queryReplica, err)
				// Continue with size 0, which will let us create a new chunk if needed
			}
		}
	}

	// Maximum append size is 1/4 of chunk size = 16MB
	maxAppendSize := CHUNK_SIZE / 4 // 16MB

	// Check if appending data might exceed the chunk capacity
	// If the current size plus the maximum possible append is close to chunk size,
	// create a new chunk to be safe
	if currentChunkSize > (CHUNK_SIZE - int64(maxAppendSize)) {
		fmt.Printf("Chunk %s is near capacity (size: %d). Creating a new chunk.\n",
			chunkId, currentChunkSize)

		// Create a new chunk
		err := master.createChunk(req.FilePath, lastChunkIndex+1)
		if err != nil {
			return nil, fmt.Errorf("failed to create new chunk: %v", err)
		}

		// Update our references to use the new chunk
		lastChunkIndex++
		chunkId = master.getChunkId(req.FilePath, lastChunkIndex)

		// Get the metadata for the new chunk
		chunkMeta, err = master.getChunkMeta(chunkId)
		if err != nil {
			return nil, err
		}

		chunkMeta.mutex.RLock()
		replicas = chunkMeta.replicas
		chunkMeta.mutex.RUnlock()
	}

	// If no replicas available, report error
	if len(replicas) == 0 {
		return nil, status.Errorf(codes.Unavailable, "No replicas available for chunk %s", chunkId)
	}

	// Select primary if none exists or lease expired
	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	if chunkMeta.primary == "" || time.Now().After(chunkMeta.leaseExpiry) {
		// Try each replica until we find one that accepts the lease
		var newPrimary string
		var leaseAssignedTime time.Time

		for _, replicaAddr := range replicas {
			client, err := master.getChunkServerClient(replicaAddr)
			if err != nil {
				fmt.Printf("Error getting client for %s: %v\n", replicaAddr, err)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
			defer cancel()

			secondaries := make([]string, 0, len(replicas))
			secondaries = append(secondaries, replicas...)

			// Increase chunk version
			leaseAssignedTime = time.Now()

			resp, err := client.GrantLeaseRPC(ctx, &chunkserverpb.GrantLeaseReq{
				ChunkId:     chunkId,
				Version:     chunkMeta.version,
				Secondaries: secondaries,
				LeaseTime:   leaseAssignedTime.Add(60 * time.Second).Unix(),
			})

			if err == nil && resp != nil && resp.Success {
				newPrimary = replicaAddr
				break
			}
		}

		if newPrimary == "" {
			return nil, status.Error(codes.Unavailable, "Could not assign primary replica")
		}

		chunkMeta.primary = newPrimary
		chunkMeta.leaseExpiry = leaseAssignedTime.Add(60 * time.Second)
		chunkMeta.version++

		// Send the update version request to all chunkservers
		err := master.sendUpdateChunkVersion(chunkId, chunkMeta.replicas)
		if err != nil {
			return nil, err
		}

		// Log the lease grant
		if err := master.opLog.LogGrantLease(chunkId, newPrimary, chunkMeta.leaseExpiry); err != nil {
			log.Printf("Failed to log lease grant: %v", err)
		}

		// Log the version update
		if err := master.opLog.LogUpdateChunkVersion(chunkId, chunkMeta.version); err != nil {
			log.Printf("Failed to log chunk version update: %v", err)
		}
	}

	return &masterpb.AppendRes{
		ChunkId:        chunkId,
		ChunkVersion:   chunkMeta.version,
		PrimaryAddress: chunkMeta.primary,
		Replicas:       replicas,
		ChunkIndex:     lastChunkIndex,
	}, nil
}

////////////////// END of Append Operation /////////////////////////////////////////////////////

////////////////////// READ Operation ////////////////////////////////////

func (master *MasterServer) GetChunkDetailsForReadRPC(ctx context.Context, req *masterpb.ReadReq) (*masterpb.ReadRes, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Request cannot be nil")
	}

	if req.FilePath == "" {
		return nil, status.Error(codes.InvalidArgument, "File path cannot be empty")
	}

	// Check if file exists
	if exists := master.fileExists(req.FilePath); !exists {
		return nil, status.Errorf(codes.NotFound, "File %s not found", req.FilePath)
	}

	// Get file metadata
	fileMetaInterface, _ := master.namespace.Load(req.FilePath)
	fileMeta := fileMetaInterface.(*FileMetadata)

	fileMeta.mutex.RLock()
	chunkCount := len(fileMeta.chunks)
	fileMeta.mutex.RUnlock()

	if chunkCount == 0 {
		return nil, status.Errorf(codes.NotFound, "File %s has no chunks", req.FilePath)
	}

	// If chunkIndex is out of bounds, return error
	if req.ChunkIndex < 0 || req.ChunkIndex >= int64(chunkCount) {
		return nil, status.Errorf(codes.OutOfRange, "Chunk index %d out of range (file has %d chunks)",
			req.ChunkIndex, chunkCount)
	}

	// Get the chunk ID for the requested chunk index
	chunkId := master.getChunkId(req.FilePath, req.ChunkIndex)
	if chunkId == "" {
		return nil, status.Errorf(codes.Internal, "Failed to get chunk ID for %s at index %d",
			req.FilePath, req.ChunkIndex)
	}

	// Get locations of the chunk replicas
	chunkMeta, err := master.getChunkMeta(chunkId)
	if err != nil {
		return nil, err
	}

	chunkMeta.mutex.RLock()
	replicas := chunkMeta.replicas
	chunkMeta.mutex.RUnlock()

	if len(replicas) == 0 {
		return nil, status.Errorf(codes.Internal, "No replicas found for chunk %s", chunkId)
	}

	// Return chunk information
	return &masterpb.ReadRes{
		ChunkId:    chunkId,
		Locations:  replicas,
		ChunkIndex: req.ChunkIndex,
	}, nil
}

////////////////////// END of READ Operation////////////////////////////////////

////////////////// Write Operation ////////////////////////////////////////////////////////////

func (master *MasterServer) getReplicasForChunk(replicaCount int, excludeAddrs []string) ([]string, error) {
	// Create set of addresses to exclude
	excludeSet := make(map[string]bool)
	for _, addr := range excludeAddrs {
		excludeSet[addr] = true
	}

	availableChunkServers := make([]*ChunkServerState, 0)
	master.chunkServers.Range(func(key any, value any) bool {
		chunkServer, ok := value.(*ChunkServerState)
		if !ok {
			return true
		}

		// Skip if excluded or not active
		if excludeSet[chunkServer.address] || chunkServer.status != "ACTIVE" {
			return true
		}

		availableChunkServers = append(availableChunkServers, chunkServer)
		return true
	})

	if len(availableChunkServers) == 0 {
		return nil, status.Error(codes.NotFound, "No active chunk servers available")
	}

	// Sort by load (fewest chunks first) and then by available disk space
	sort.Slice(availableChunkServers, func(i, j int) bool {
		if len(availableChunkServers[i].chunks) != len(availableChunkServers[j].chunks) {
			return len(availableChunkServers[i].chunks) < len(availableChunkServers[j].chunks)
		}
		return availableChunkServers[i].diskSpace > availableChunkServers[j].diskSpace
	})

	// Select top N servers
	topLeastLoadServers := make([]string, 0)
	count := min(replicaCount, len(availableChunkServers))
	for i := 0; i < count; i++ {
		topLeastLoadServers = append(topLeastLoadServers, availableChunkServers[i].address)
	}

	fmt.Printf("Selected target servers for re-replication (excluding %v): %v\n", excludeAddrs, topLeastLoadServers)
	return topLeastLoadServers, nil
}

func (master *MasterServer) createChunk(filePath string, chunkIndex int64) error {
	fileMetaInterface, _ := master.namespace.Load(filePath)
	fileMeta, ok := fileMetaInterface.(*FileMetadata)
	if !ok {
		return status.Errorf(codes.Internal, "Invalid file metadata type for %s", filePath)
	}

	fileMeta.mutex.Lock()
	defer fileMeta.mutex.Unlock()

	currLength := len(fileMeta.chunks)
	for i := currLength; i <= int(chunkIndex); i++ {
		chunkId := generateChunkId()

		// Create a new ChunkMeta
		chunkMeta := &ChunkMeta{
			version:  0,
			replicas: make([]string, 0),
		}
		master.chunkMetadata.Store(chunkId, chunkMeta)

		// Assign replicas
		replicas, err := master.getReplicasForChunk(REPLICA_COUNT, nil)
		if err != nil {
			return err
		}

		// Flag to track if the chunk was successfully added to at least one replica
		chunkAddedToReplica := false
		for _, addrress := range replicas {
			client, err := master.getChunkServerClient(addrress)
			if err != nil {
				fmt.Printf("Failed to get client for replica %s to assign chunk\n", addrress)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
			res, err := client.AssignChunkRPC(ctx, &chunkserverpb.AssignChunkReq{ChunkId: chunkId, Version: 0})
			cancel()

			if err != nil || !res.Success {
				fmt.Printf("Failed to assign chunk to replica %s\n", addrress)
			}

			master.addChunkToReplica(addrress, chunkId, 0)
			err = master.addReplicaToChunk(chunkId, addrress)
			if err != nil {
				fmt.Printf("Failed to add replica %s: %v", addrress, err)
			}
			chunkAddedToReplica = true
		}

		// Only add the chunk to the file if it was successfully added to at least one replica
		if chunkAddedToReplica {
			fileMeta.chunks = append(fileMeta.chunks, chunkId)

			// Log the chunk creation
			if err := master.opLog.LogCreateChunk(filePath, int64(i), chunkId); err != nil {
				log.Printf("Failed to log chunk creation: %v", err)
			}
		} else {
			// If no replica was successful clean up chunkMeta
			master.chunkMetadata.Delete(chunkId)
			return status.Errorf(codes.Internal, "Failed to assign chunk to any replica servers")
		}
	}
	return nil
}

func (master *MasterServer) sendUpdateChunkVersion(chunkId string, replicas []string) error {
	for _, replicaAddr := range replicas {
		client, err := master.getChunkServerClient(replicaAddr)
		if err != nil {
			fmt.Printf("Error getting client for replica %s: %v\n", replicaAddr, err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
		res, err := client.UpdateChunkVersionRPC(ctx, &chunkserverpb.UpdateChunkVersionReq{ChunkId: chunkId})
		cancel()
		if err != nil || !res.Success {
			return err
		}
	}
	return nil
}

func (master *MasterServer) GetChunkDetailsForWriteRPC(ctx context.Context, req *masterpb.WriteReq) (*masterpb.WriteRes, error) {

	// Check if we're in read-only mode
	if err := master.allowWriteOperation(); err != nil {
		return nil, err
	}

	if req.FilePath == "" {
		return nil, status.Error(codes.InvalidArgument, "File path cannot be empty")
	}

	// check whether file exists
	if exists := master.fileExists(req.FilePath); !exists {
		return nil, status.Errorf(codes.NotFound, "File %s not found", req.FilePath)
	}

	// get ChunkId
	chunkId := master.getChunkId(req.FilePath, req.ChunkIndex)
	if chunkId == "" {
		// Create chunk if it doesn't exist
		err := master.createChunk(req.FilePath, req.ChunkIndex)
		if err != nil {
			return nil, err
		}

		// Get the newly created chunk ID
		chunkId = master.getChunkId(req.FilePath, req.ChunkIndex)
		if chunkId == "" {
			return nil, status.Errorf(codes.Internal, "Failed to create chunk for %s at index %d", req.FilePath, req.ChunkIndex)
		}
	}

	// Get chunkMeta
	chunkMeta, err := master.getChunkMeta(chunkId)
	if err != nil {
		return nil, err
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	// If no replicas available, report error
	if len(chunkMeta.replicas) == 0 {
		return nil, status.Errorf(codes.Unavailable, "No replicas available for chunk %s", chunkId)
	}

	// Select primary if none exists or lease expired
	if chunkMeta.primary == "" || time.Now().After(chunkMeta.leaseExpiry) {
		// Try each replica until we find one that accepts the lease
		var newPrimary string
		var leaseAssignedTime time.Time

		for _, replicaAddr := range chunkMeta.replicas {
			client, err := master.getChunkServerClient(replicaAddr)
			if err != nil {
				fmt.Printf("Error getting client for replica %s: %v\n", replicaAddr, err)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)

			// Prepare secondaries list (excluding current replica)
			secondaries := make([]string, 0, len(chunkMeta.replicas)-1)
			for _, addr := range chunkMeta.replicas {
				if addr != replicaAddr {
					secondaries = append(secondaries, addr)
				}
			}

			fmt.Println(secondaries)

			leaseAssignedTime = time.Now()

			res, err := client.GrantLeaseRPC(ctx, &chunkserverpb.GrantLeaseReq{
				ChunkId:     chunkId,
				Version:     chunkMeta.version,
				Secondaries: secondaries,
				LeaseTime:   60,
			})
			cancel()

			if err != nil || !res.Success {
				fmt.Printf("Failed to assign replica %s as primary: %v\n", replicaAddr, err)
			}

			if res.Success {
				newPrimary = replicaAddr
				break
			}
		}

		if newPrimary == "" {
			return nil, status.Error(codes.Unavailable, "Could not assign primary replica")
		}

		chunkMeta.primary = newPrimary
		chunkMeta.leaseExpiry = leaseAssignedTime.Add(60 * time.Second)
		chunkMeta.version = chunkMeta.version + 1
		// send the update version request to all chunkservers
		err := master.sendUpdateChunkVersion(chunkId, chunkMeta.replicas)
		if err != nil {
			return nil, err
		}

		// Log the version update
		if err := master.opLog.LogUpdateChunkVersion(chunkId, chunkMeta.version); err != nil {
			log.Printf("Failed to log chunk version update: %v", err)
		}

		// Log the lease grant
		if err := master.opLog.LogGrantLease(chunkId, newPrimary, chunkMeta.leaseExpiry); err != nil {
			log.Printf("Failed to log lease grant: %v", err)
		}
	}

	return &masterpb.WriteRes{
		ChunkId:        chunkId,
		ChunkVersion:   chunkMeta.version,
		PrimaryAddress: chunkMeta.primary,
		Replicas:       chunkMeta.replicas,
	}, nil
}

////////////////// END of Write Operation /////////////////////////////////////////////////////

////////////////// Create File or Directory ////////////////////////////////////////////////////////////////////

func (master *MasterServer) CreateDirectoryRPC(ctx context.Context, req *masterpb.CreateDirectoryReq) (*masterpb.CreateDirectoryRes, error) {

	// Check if we're in read-only mode
	if err := master.allowWriteOperation(); err != nil {
		return nil, err
	}

	path := req.DirectoryPath
	// validate path
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "Directory path cannot be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return nil, status.Error(codes.InvalidArgument, "Path must be absolute")
	}

	master.acquireLocksOnNamespace(path, true) // write lock on new directory
	defer master.releaseLocksOnNamespace(path, true)

	// check if directory already exists
	if _, exists := master.namespace.Load(path); exists {
		return nil, status.Errorf(codes.AlreadyExists, "%s directory already exists", path)
	}

	// check if parent directory exist or not
	parentDirectory := filepath.Dir(path)
	if parentDirectory == "" {
		parentDirectory = "/"
	}

	parentMetaInterface, exists := master.namespace.Load(parentDirectory)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "%s directory does not exist", parentDirectory)
	}

	// check whether parent is a directory or not
	parentMeta, ok := parentMetaInterface.(*FileMetadata)
	if !ok {
		return nil, status.Errorf(codes.Internal, "Invalid metadata type for %s", parentDirectory)
	}

	if !parentMeta.isDir {
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a directory", parentDirectory)
	}

	now := time.Now()
	dirMeta := &FileMetadata{
		isDir:    true,
		children: &sync.Map{},
		created:  now,
		modified: now,
	}

	// add the new directory name as children to parentMeta
	parentMeta.mutex.Lock()
	parentMeta.children.Store(filepath.Base(path), true)
	parentMeta.modified = now
	parentMeta.mutex.Unlock()

	// store this new directory in namespace
	master.namespace.Store(path, dirMeta)

	// Log the operation
	if err := master.opLog.LogCreateDirectory(req.DirectoryPath); err != nil {
		log.Printf("Failed to log directory creation: %v", err)
		return &masterpb.CreateDirectoryRes{Success: false}, fmt.Errorf("failed to log operation")
	}

	master.printAllnamespaces()

	return &masterpb.CreateDirectoryRes{Success: true}, nil
}

func (master *MasterServer) CreateFileRPC(ctx context.Context, req *masterpb.CreateFileReq) (*masterpb.CreateFileRes, error) {
	// Check if we're in read-only mode
	if err := master.allowWriteOperation(); err != nil {
		return nil, err
	}

	path := req.FilePath
	// validate path
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "File path cannot be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return nil, status.Error(codes.InvalidArgument, "Path must be absolute")
	}

	master.acquireLocksOnNamespace(path, true) // write lock on new file
	defer master.releaseLocksOnNamespace(path, true)

	// check if file already exists
	if metaInterface, exists := master.namespace.Load(path); exists {
		meta, ok := metaInterface.(*FileMetadata)
		if !ok {
			return nil, status.Errorf(codes.Internal, "Invalid metadata type for %s", path)
		}

		if meta.isDir {
			return nil, status.Errorf(codes.InvalidArgument, "%s is a directory, choose another filename", path)
		} else {
			return nil, status.Errorf(codes.AlreadyExists, "%s file already exists", path)
		}
	}

	// check if parent directory exist or not
	parentDirectory := filepath.Dir(path)
	if parentDirectory == "" {
		parentDirectory = "/"
	}
	parentMetaInterface, exists := master.namespace.Load(parentDirectory)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "%s directory does not exist", parentDirectory)
	}

	// check whether parent is a directory or not
	parentMeta, ok := parentMetaInterface.(*FileMetadata)
	if !ok {
		return nil, status.Errorf(codes.Internal, "Invalid metadata type for %s", parentDirectory)
	}

	if !parentMeta.isDir {
		return nil, status.Errorf(codes.InvalidArgument, "%s is not a directory", parentDirectory)
	}

	now := time.Now()
	fileMeta := &FileMetadata{
		isDir:    false,
		chunks:   make([]string, 0),
		created:  now,
		modified: now,
	}

	// add the new file name as children to parentMeta
	parentMeta.mutex.Lock()
	parentMeta.children.Store(filepath.Base(path), true)
	parentMeta.modified = now
	parentMeta.mutex.Unlock()

	// store this new file in namespace
	master.namespace.Store(path, fileMeta)

	// Log the operation
	if err := master.opLog.LogCreateFile(req.FilePath); err != nil {
		log.Printf("Failed to log file creation: %v", err)
		return &masterpb.CreateFileRes{Success: false}, fmt.Errorf("failed to log operation")
	}

	master.printAllnamespaces()

	return &masterpb.CreateFileRes{Success: true}, nil
}

////////////////// END of Create File or Directory /////////////////////////////////////////////////////////////

////////////////// Delete File functions //////////////////////////////////////////////////////////

func (master *MasterServer) UndeleteFileRPC(ctx context.Context, req *masterpb.UndeleteFileReq) (*masterpb.UndeleteFileRes, error) {
	path := req.FilePath
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "File path cannot be empty")
	}

	master.acquireLocksOnNamespace(path, true)
	defer master.releaseLocksOnNamespace(path, true)

	// Check if file is in deletion queue
	_, exists := master.deleteFilesQueue.Load(path + "hidden")
	if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "File %s is not marked for deletion", path)
	}

	// Check if original path is available
	if _, exists := master.namespace.Load(path); exists {
		return nil, status.Errorf(codes.AlreadyExists, "File %s already exists", path)
	}

	// Rename back to original path
	hiddenPath := path + "hidden"
	err := master.renameFilePath(hiddenPath, path)
	if err != nil {
		return nil, err
	}

	// Remove from deletion queue
	master.deleteFilesQueue.Delete(hiddenPath)

	return &masterpb.UndeleteFileRes{Success: true}, nil
}

func (master *MasterServer) renameFilePath(currentPath, newPath string) error {
	// Get the file metadata
	fileMetaInterface, exists := master.namespace.Load(currentPath)
	if !exists {
		return status.Errorf(codes.NotFound, "File %s not found", currentPath)
	}

	fileMeta, ok := fileMetaInterface.(*FileMetadata)
	if !ok {
		return status.Errorf(codes.Internal, "Invalid metadata type for %s", currentPath)
	}

	// Remove from parent directory
	parentPath := filepath.Dir(currentPath)
	if parentPath == "" {
		parentPath = "/"
	}

	parentMetaInterface, exists := master.namespace.Load(parentPath)
	if !exists {
		return status.Errorf(codes.NotFound, "Parent directory %s not found", parentPath)
	}

	parentMeta, ok := parentMetaInterface.(*FileMetadata)
	if !ok || !parentMeta.isDir {
		return status.Errorf(codes.Internal, "Invalid parent directory type for %s", parentPath)
	}

	// Remove old entry from parent
	parentMeta.mutex.Lock()
	parentMeta.children.Delete(filepath.Base(currentPath))
	parentMeta.mutex.Unlock()

	// Add new entry to parent
	parentMeta.mutex.Lock()
	parentMeta.children.Store(filepath.Base(newPath), true)
	parentMeta.mutex.Unlock()

	// Store under new path
	master.namespace.Store(newPath, fileMeta)
	master.namespace.Delete(currentPath)

	return nil
}

func (master *MasterServer) DeleteFileRPC(ctx context.Context, req *masterpb.DeleteFileReq) (*masterpb.DeleteFileRes, error) {
	path := req.FilePath
	// validate path
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "File path cannot be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return nil, status.Error(codes.InvalidArgument, "Path must be absolute")
	}

	master.acquireLocksOnNamespace(path, true) // write lock on file
	defer master.releaseLocksOnNamespace(path, true)

	// check whether file exists
	if exists := master.fileExists(path); !exists {
		return nil, status.Errorf(codes.NotFound, "File %s not found", path)
	}

	// Rename the file to hidden path
	hiddenPath := path + "hidden"
	err := master.renameFilePath(path, hiddenPath)
	if err != nil {
		return nil, err
	}

	// add this to delete files queue
	master.deleteFilesQueue.Store(hiddenPath, time.Now())

	return &masterpb.DeleteFileRes{Success: true}, nil
}

////////////////// End of Delete File functions //////////////////////////////////////////////////////////

///////////////// Heartbeat Mechanism //////////////////////////////////////////////////////////////////////////

// When chunk servers send heartbat, this function updates chunk server state and manages replicas of chunks
func (master *MasterServer) processHeartbeat(req *masterpb.HeartbeatReq) {
	now := time.Now()

	// Get current chunks reported by this chunkserver
	currReportedChunks := make(map[string]bool)
	for _, chunk := range req.Chunks {
		currReportedChunks[chunk.ChunkId] = true
	}

	// Load or create chunkserver state
	csState, _ := master.chunkServers.LoadOrStore(req.Address, &ChunkServerState{
		address:       req.Address,
		status:        "ACTIVE",
		lastHeartBeat: now,
		diskSpace:     req.DiskSpace,
		chunks:        make(map[string]int64),
	})

	state, ok := csState.(*ChunkServerState)
	if !ok {
		log.Printf("Invalid chunkserver state type for %s", req.Address)
		return
	}

	// Get prev chunks held by this chunkserver
	prevChunks := make(map[string]bool)
	state.mutex.RLock()
	for chunkId := range state.chunks {
		prevChunks[chunkId] = true
	}
	state.mutex.RUnlock()

	// Add this server to newly repoted chunks
	for chunkId := range currReportedChunks {
		err := master.addReplicaToChunk(chunkId, req.Address)
		if err != nil {
			log.Printf("Failed to add replica %s to chunk %s: %v\n", req.Address, chunkId, err)
		}
	}

	// Remove this server from chunks that no longer reported
	for chunkId := range prevChunks {
		if !currReportedChunks[chunkId] {
			fmt.Printf("Removing chunkserver %s from chunk %s due to chunkserver hasn't reported\n", req.Address, chunkId)
			err := master.removeReplicaFromChunk(chunkId, req.Address)
			if err != nil {
				log.Printf("Failed to remove replica %s from chunk %s: %v\n", req.Address, chunkId, err)
			}
		}
	}

	// Update ChunkServer state
	state.mutex.Lock()
	defer state.mutex.Unlock()

	//Initialize conncetion if doesn't exist
	if state.conn == nil {
		conn, client, err := connectToServer(req.Address)
		if err != nil {
			log.Printf("Failed to connect to chunk server %s: %v\n", req.Address, err)
		} else {
			state.conn = conn
			state.client = client
		}
	}

	state.status = "ACTIVE"
	state.lastHeartBeat = now
	state.diskSpace = req.DiskSpace
	state.chunks = make(map[string]int64)
	for _, chunk := range req.Chunks {
		state.chunks[chunk.ChunkId] = chunk.Version
	}
}

func (master *MasterServer) checkVersionMismatchChunkErrors(chunks []*masterpb.Chunk, address string) {
	// Check for version mismatches and disk errors
	for _, chunk := range chunks {
		chunkMeta, err := master.getChunkMeta(chunk.ChunkId)
		if err != nil {
			continue
		}

		chunkMeta.mutex.RLock()
		currentVersion := chunkMeta.version
		chunkMeta.mutex.RUnlock()

		// Check for version mismatch
		if chunk.Version < currentVersion {
			fmt.Printf("Removing chunkserver %s from chunk %s due to version mismatch\n", address, chunk.ChunkId)
			master.removeReplicaFromChunk(chunk.ChunkId, address)
			master.queueChunkForReReplication(chunk.ChunkId, REPLICA_COUNT)
			client, err := master.getChunkServerClient(address)
			if err != nil {
				fmt.Printf("Failed to handle stale replica: %v\n", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
			defer cancel()
			res, err := client.DeleteChunkRPC(ctx, &chunkserverpb.DeleteChunkReq{ChunkID: chunk.ChunkId})
			if err != nil || !res.Success {
				fmt.Printf("Failed to handle stale replica: %v\n", err)
			}
		}

		// Check for disk errors
		if chunk.HasError {
			fmt.Printf("Removing chunkserver %s from chunk %s due to error reported by chunkserver\n", address, chunk.ChunkId)
			master.removeReplicaFromChunk(chunk.ChunkId, address)
			master.queueChunkForReReplication(chunk.ChunkId, REPLICA_COUNT)
		}
	}
}

// method to recive HeartbeatRPC from chunkservers
func (master *MasterServer) HeartbeatRPC(ctx context.Context, req *masterpb.HeartbeatReq) (*masterpb.HeartbeatRes, error) {

	// fmt.Printf("Received heartbeat from chunkserver %s and reported chunks are: %v\n", req.Address, req.Chunks)

	if master.isPrimary {
		master.processHeartbeat(req)

		master.checkVersionMismatchChunkErrors(req.Chunks, req.Address)
	}

	return &masterpb.HeartbeatRes{Success: true}, nil
}

///////////////// END of Heartbeat Mechanism ///////////////////////////////////////////////

///////////////// Master server setup functions //////////////////////////////////////////////////////

// Helper function to extract method name from full method path
func extractMethodName(fullMethod string) string {
	parts := strings.Split(fullMethod, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return fullMethod
}

// Log request and responces of master RPC calls
func loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	methodName := extractMethodName(info.FullMethod)
	var logRequest, logResponse string
	logRequest = fmt.Sprintf("Methodname: %s | Request: %v\n", methodName, req)
	log.Printf("→ [Master Request] %s", logRequest)
	res, err := handler(ctx, req)
	// Capture error and response details
	if err != nil {
		st, _ := status.FromError(err)
		logResponse = fmt.Sprintf("Methodname: %s | status: Fail | ErrorCode: %v| ErrorMessage: %v", methodName, st.Code(), st.Message())
	} else {
		logResponse = fmt.Sprintf("Methodname: %s | status: Success | Response: %v\n", methodName, res)
	}

	log.Printf("← [Master Response] %s", logResponse)
	return res, err
}

// Add this function to main.go after the main function or where appropriate
func registerWithPrimary(shadowAddress, primaryAddress string) error {
	// Connect to primary master
	conn, err := grpc.NewClient("localhost:"+primaryAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to primary master: %v", err)
	}

	client := masterpb.NewMasterServiceClient(conn)

	// Register with primary
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.RegisterShadowMasterRPC(ctx, &masterpb.RegisterShadowMasterReq{
		Address: shadowAddress,
	})

	if err != nil {
		return fmt.Errorf("failed to register with primary: %v", err)
	}

	if !res.Success {
		return fmt.Errorf("primary master refused registration")
	}

	log.Printf("Successfully registered with primary master. Current log sequence: %d", res.CurrentLogSeq)
	return nil
}

func (master *MasterServer) initializeEtcd() error {
    config := clientv3.Config{
        Endpoints:   []string{"localhost:2379"}, 
        DialTimeout: 5 * time.Second,
    }
    
    client, err := clientv3.New(config)
    if err != nil {
        return fmt.Errorf("failed to connect to etcd: %v", err)
    }
    
    master.etcdClient = client
    master.electionKey = "gfs/election"
    master.primaryKey = "gfs/primary"
    master.secondaryKey = "gfs/secondary"
    master.electionTTL = 10 // 10 seconds TTL
    
    // Create session with TTL
    session, err := concurrency.NewSession(client, concurrency.WithTTL(master.electionTTL))
    if err != nil {
        return fmt.Errorf("failed to create etcd session: %v", err)
    }
    master.etcdSession = session
    
    return nil
}

func (master *MasterServer) cleanupEtcd() {
    if master.etcdSession != nil {
        master.etcdSession.Close()
    }
    if master.etcdClient != nil {
        master.etcdClient.Close()
    }
}

// Create a tcp listener for master server
func createTCPListener() net.Listener {
	listener, err := net.Listen("tcp", ":"+basePort)
	if err != nil {
		log.Fatalf("Failed to listen: %v\n", err)
	}
	return listener
}

func init() {
	flag.StringVar(&basePort, "basePort", "50050", "Base port of the application")
	flag.IntVar(&numChunkServers, "N", 10, "Number of chunk servers")
	flag.BoolVar(&isShadowMaster, "shadow", false, "Run as shadow master")
	flag.StringVar(&primaryMasterAddr, "primary", "", "Address of primary master (required for shadow)")
	flag.Parse()
	fmt.Printf("Parsed flags: basePort=%s, numChunkServers=%d, isShadowMaster=%v, primaryMasterAddr=%s\n",
		basePort, numChunkServers, isShadowMaster, primaryMasterAddr)
}

///////////////// END of Master server setup functions //////////////////////////////////////////////

func main() {
	listener := createTCPListener()
	defer listener.Close()
	master := NewMasterServer(basePort)
	fmt.Println("Master server object created")

	// Recover state from logs before initializing shadow master support
    if err := master.recoverStateFromLogs(); err != nil {
        log.Printf("Warning: error recovering state from logs: %v", err)
    }

	master.initializeShadowMasterSupport(isShadowMaster)
	fmt.Println("Shadow master support initialized")

	if master.isPrimary {
		// Start chunk server monitoring
		go master.handleFailuresRereplicationGarbageCollection()
	}

	// Create grpc server and register master with masterService
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingInterceptor),
	)
	masterpb.RegisterMasterServiceServer(grpcServer, master)

	fmt.Printf("Master server is running on port %s\n", master.address)
	fmt.Println("Master isPrimary:", master.isPrimary)

	if isShadowMaster {
		if primaryMasterAddr == "" {
			log.Fatal("Shadow master requires primary address")
		}

		// Wait for primary to be ready
		time.Sleep(2 * time.Second)

		// Register with primary master with retries
		maxRetries := 5
		for i := 0; i < maxRetries; i++ {
			err := registerWithPrimary(basePort, primaryMasterAddr)
			if err == nil {
				log.Printf("Successfully registered with primary master")
				break
			}
			if i == maxRetries-1 {
				log.Fatalf("Failed to register with primary master after %d attempts: %v", maxRetries, err)
			}
			log.Printf("Failed to register with primary (attempt %d/%d): %v", i+1, maxRetries, err)
			time.Sleep(2 * time.Second)
		}

		// Start monitoring the primary master
		//go master.monitorPrimaryMaster()
	}

	// Set up clean shutdown
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c
        fmt.Println("Shutting down...")
        master.cleanupEtcd()
        grpcServer.GracefulStop()
        os.Exit(0)
    }()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v\n", err)
	}
}
