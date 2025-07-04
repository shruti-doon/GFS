package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	chunkserverpb "github.com/laharisiri123/Distributed-Google-File-System-Project/protofiles/chunkserver"
	masterpb "github.com/laharisiri123/Distributed-Google-File-System-Project/protofiles/master"
	"github.com/shirou/gopsutil/disk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	port               string
	masterAddr         string
	masterClient       masterpb.MasterServiceClient
	dataBasepath       = "./data"
	chunksDirectory    string
	masterAddresses    []string
	currentMasterIndex int
	etcdClient     *clientv3.Client
    etcdEndpoints  = []string{"localhost:2379"} 
)

const (
	HEARTBEAT_INTERVAL            = time.Second * 1
	RPC_TIMEOUT_INTERVAL          = time.Second * 10
	PENDING_WRITE_WAIT_INTERVAL   = time.Second * 9
	PENDIND_WRITE_TICKER_INTERVAL = time.Millisecond * 100
)

type ChunkServer struct {
	address        string
	chunks         *sync.Map // chunkId -> chunkMeta
	writeBuffers   *sync.Map // chunkId-clientAddr -> WriteBufferMeta
	pendingAppends *sync.Map // chunkId-clientAddr -> PendingAppend
	chunkserverpb.UnimplementedChunkserverServiceServer
}

type ChunkMeta struct {
	serialNumber      int64
	nextSerialNumber  int64 // Next serial number to assign (primary only)
	version           int64
	isPrimary         bool
	secondaries       []string
	leaseExpiry       time.Time
	lastModified      time.Time
	lastAppliedSerial int64 // Last serial number successfully applied
	mutex             sync.RWMutex
}

type WriteBufferMeta struct {
	data    string
	offset  int64
	version int64
}

type PendingAppend struct {
	data         string
	serialNumber int64
	applied      bool
}

func NewChunkServer(address string) *ChunkServer {
	return &ChunkServer{
		address:        address,
		chunks:         &sync.Map{},
		writeBuffers:   &sync.Map{},
		pendingAppends: &sync.Map{},
	}
}

func getServerClient(address string) (chunkserverpb.ChunkserverServiceClient, error) {
	// Establish connection to the gRPC server
	conn, err := grpc.NewClient("localhost:"+address, grpc.WithInsecure())
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to connect to chunkserver %s: %v", address, err)
	}

	client := chunkserverpb.NewChunkserverServiceClient(conn)
	return client, nil
}

func connectToServer(address string) (*grpc.ClientConn, chunkserverpb.ChunkserverServiceClient, error) {
	conn, err := grpc.NewClient("localhost:"+address, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to chunkserver with address %s: %v\n", address, err)
		return nil, nil, err
	}
	client := chunkserverpb.NewChunkserverServiceClient(conn)
	return conn, client, nil
}

// Initialize etcd client
func initEtcdClient() error {
    config := clientv3.Config{
        Endpoints:   etcdEndpoints,
        DialTimeout: 5 * time.Second,
    }
    
    client, err := clientv3.New(config)
    if err != nil {
        return fmt.Errorf("failed to connect to etcd: %v", err)
    }
    
    etcdClient = client
    return nil
}

// Get current master address from etcd
func getMasterFromEtcd() (string, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    
    resp, err := etcdClient.Get(ctx, "gfs/primary")
    if err != nil {
        return "", fmt.Errorf("failed to get primary master from etcd: %v", err)
    }
    
    if len(resp.Kvs) == 0 {
        return "", fmt.Errorf("no primary master registered in etcd")
    }
    
    return string(resp.Kvs[0].Value), nil
}

// Watch for changes in the master address
func watchMasterChanges() {
    watchChan := etcdClient.Watch(context.Background(), "gfs/primary", clientv3.WithPrefix())
    
    for watchResp := range watchChan {
        for _, event := range watchResp.Events {
            if event.Type == clientv3.EventTypePut {
                // Master address has changed
                newMaster := string(event.Kv.Value)
                log.Printf("Primary master changed to %s", newMaster)
                
                // Connect to the new master
                conn, err := grpc.Dial("localhost:"+newMaster, grpc.WithInsecure())
                if err != nil {
                    log.Printf("Failed to connect to new master at %s: %v", newMaster, err)
                    continue
                }
                
                masterClient = masterpb.NewMasterServiceClient(conn)
                currentMasterIndex = findMasterIndex(newMaster)
                log.Printf("Successfully connected to new master at %s", newMaster)
            }
        }
    }
}

// Find the index of a master address in our list
func findMasterIndex(address string) int {
    for i, addr := range masterAddresses {
        if addr == address {
            return i
        }
    }
    
    // If not found, just use the first one
    return 0
}

// Get chunk meta
func (server *ChunkServer) getChunkMeta(chunkId string) (*ChunkMeta, error) {
	chunkMetaInterface, exists := server.chunks.Load(chunkId)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "chunk %s Not found", chunkId)
	}
	chunkMeta, ok := chunkMetaInterface.(*ChunkMeta)
	if !ok {
		return nil, status.Errorf(codes.Internal, "Invalid chunk metadata type for %s", chunkId)
	}
	return chunkMeta, nil
}

func (server *ChunkServer) DeleteChunkRPC(ctx context.Context, req *chunkserverpb.DeleteChunkReq) (*chunkserverpb.DeleteChunkRes, error) {
	// Check whether chunk exists or not
	_, exists := server.chunks.Load(req.ChunkID)
	if exists {
		server.chunks.Delete(req.ChunkID)
	}

	return &chunkserverpb.DeleteChunkRes{Success: true}, nil
}

func (server *ChunkServer) CloneChunkRPC(ctx context.Context, req *chunkserverpb.CloneChunkReq) (*chunkserverpb.CloneChunkRes, error) {
	chunkId := req.ChunkId
	sourceServer := req.SourceServer

	// Connect to Source Server
	sourceClient, err := getServerClient(sourceServer)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Failed to connect to source server: %v", err)
	}

	// Get chunk data from source server
	getChunkDataCtx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()
	res, err := sourceClient.GetChunkInfoAndDataRPC(getChunkDataCtx, &chunkserverpb.GetChunkInfoAndDataReq{ChunkId: chunkId})

	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get data from Source server: %v", err)
	}

	// If successfully got data check the version is matched with master informed
	if req.Version > res.Version {
		return nil, status.Errorf(codes.InvalidArgument, "Stale replica data got from source server: Version received is %d but requires version is %d", res.Version, req.Version)
	}

	// If everything ok, then store chunk details
	now := time.Now()
	server.chunks.Store(req.ChunkId, &ChunkMeta{
		version:           res.Version,
		isPrimary:         false,
		lastModified:      now,
		serialNumber:      res.SerialNumber,
		nextSerialNumber:  res.NextSerialNumber,
		lastAppliedSerial: res.LastAppendSerial,
	})

	// Create file and add data
	// Write to file
	filepath := filepath.Join(chunksDirectory, chunkId)
	if err := writeTofileAtOfsset(filepath, res.Data, 0); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to copy data: %v", err)
	}

	return &chunkserverpb.CloneChunkRes{Success: true}, nil
}

func (server *ChunkServer) GetChunkInfoAndDataRPC(ctx context.Context, req *chunkserverpb.GetChunkInfoAndDataReq) (*chunkserverpb.GetChunkInfoAndDataRes, error) {
	chunkId := req.ChunkId
	chunkMeta, err := server.getChunkMeta(chunkId)
	if err != nil {
		return nil, err
	}

	filepath := filepath.Join(chunksDirectory, chunkId)
	data, err := readDataFromChunk(filepath)
	if err != nil {
		return nil, err
	}

	return &chunkserverpb.GetChunkInfoAndDataRes{SerialNumber: chunkMeta.serialNumber, NextSerialNumber: chunkMeta.nextSerialNumber, LastAppendSerial: chunkMeta.lastAppliedSerial, Version: chunkMeta.version, Data: data}, nil
}

// readDataFromChunk reads the entire content of the file and returns it as a string
func readDataFromChunk(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (server *ChunkServer) GetChunkSizeRPC(ctx context.Context, req *chunkserverpb.GetChunkSizeReq) (*chunkserverpb.GetChunkSizeRes, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "Request cannot be nil")
	}

	chunkId := req.ChunkId

	// Check if the chunk exists
	_, exists := server.chunks.Load(chunkId)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "Chunk %s not found", chunkId)
	}

	// Get the path to the chunk file
	chunkPath := filepath.Join(chunksDirectory, chunkId)

	// Check if the file exists
	fileInfo, err := os.Stat(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			// The chunk is registered but the file doesn't exist yet
			return &chunkserverpb.GetChunkSizeRes{Size: 0}, nil
		}
		return nil, status.Errorf(codes.Internal, "Failed to get file info: %v", err)
	}

	// Return the file size
	return &chunkserverpb.GetChunkSizeRes{Size: fileInfo.Size()}, nil
}

func (server *ChunkServer) CommitAppendRPC(ctx context.Context, req *chunkserverpb.CommitAppendReq) (*chunkserverpb.CommitAppendRes, error) {
	chunkId := req.ChunkId
	clientAddr := req.ClientAddress

	// Get chunk metadata
	chunkMeta, err := server.getChunkMeta(chunkId)
	if err != nil {
		return nil, err
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	// Check if this server is the primary
	if !chunkMeta.isPrimary {
		return nil, status.Errorf(codes.FailedPrecondition, "Server %s is not the primary for chunk %s", server.address, chunkId)
	}

	// Get the pending data from the write buffer
	bufferKey := fmt.Sprintf("%s-%s", chunkId, clientAddr)

	/* fmt.Printf("Looking for buffer with key: %s\n", bufferKey)
	 // Print all buffer keys for debugging
	server.writeBuffers.Range(func(key, value interface{}) bool {
	    fmt.Printf("Available buffer key: %v\n", key)
	    return true
	}) */

	bufferMetaInterface, exists := server.writeBuffers.Load(bufferKey)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "No pending append data found for client %s on chunk %s", clientAddr, chunkId)
	}

	bufferMeta := bufferMetaInterface.(*WriteBufferMeta)

	// Assign a serial number for this append
	serialNumber := chunkMeta.nextSerialNumber
	chunkMeta.nextSerialNumber++

	chunkPath := filepath.Join(chunksDirectory, chunkId)
	fileInfo, err := os.Stat(chunkPath)
	var appendOffset int64 = 0

	if err == nil {
		// File exists, get its current size
		appendOffset = fileInfo.Size()
	}

	// Perform the append to the local chunk
	f, err := os.OpenFile(chunkPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to open chunk file: %v", err))
	}
	defer f.Close()

	// Write the data to the local chunk
	_, err = f.Write([]byte(bufferMeta.data))
	if err != nil {
		f.Close()
		return nil, status.Errorf(codes.Internal, "Failed to write data: %v", err)
	}
	f.Close()

	log.Printf("Primary %s applied append with serial number %d to chunk %s",
		server.address, serialNumber, chunkId)

	// Update the last applied serial number for the primary
	chunkMeta.lastAppliedSerial = serialNumber

	// Forward the commit to all secondaries with the assigned serial number
	allSecondariesSuccess := true
	for _, secondary := range chunkMeta.secondaries {
		_, client, err := connectToServer(secondary)
		if err != nil {
			log.Printf("Failed to connect to secondary %s: %v", secondary, err)
			allSecondariesSuccess = false
			continue
		}

		forwardCtx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
		defer cancel()

		// Forward the append with serial number to the secondary
		forwardReq := &chunkserverpb.ForwardAppendReq{
			ChunkId:       chunkId,
			ClientAddress: clientAddr,
			SerialNumber:  serialNumber,
		}

		res, err := client.ForwardAppendRPC(forwardCtx, forwardReq)
		if err != nil || !res.Success {
			log.Printf("Secondary %s failed to process forwarded append: %v", secondary, err)
			allSecondariesSuccess = false
		}
	}

	// Clean up the buffer after commit attempt
	server.writeBuffers.Delete(bufferKey)

	if !allSecondariesSuccess {
		return nil, status.Errorf(codes.Internal, "Failed to commit append on all secondaries")
	}

	return &chunkserverpb.CommitAppendRes{Success: true, Offset: appendOffset}, nil
}

// ForwardAppendRPC is called by the primary to forward an append with a serial number
func (server *ChunkServer) ForwardAppendRPC(ctx context.Context, req *chunkserverpb.ForwardAppendReq) (*chunkserverpb.ForwardAppendRes, error) {
	chunkId := req.ChunkId
	clientAddr := req.ClientAddress
	serialNumber := req.SerialNumber

	// Get chunk metadata
	chunkMeta, err := server.getChunkMeta(chunkId)
	if err != nil {
		return nil, err
	}

	// Verify this server is a secondary (not primary)
	chunkMeta.mutex.RLock()
	if chunkMeta.isPrimary {
		return nil, status.Errorf(codes.FailedPrecondition, "Server %s is the primary for chunk %s and should not receive forwarded appends", server.address, chunkId)
	}
	chunkMeta.mutex.RUnlock()

	// Get the pending data from the write buffer
	bufferKey := fmt.Sprintf("%s-%s", chunkId, clientAddr)
	bufferMetaInterface, exists := server.writeBuffers.Load(bufferKey)
	if !exists {
		return nil, status.Errorf(codes.NotFound, "No pending append data found for client %s on chunk %s", clientAddr, chunkId)
	}

	bufferMeta := bufferMetaInterface.(*WriteBufferMeta)

	// Check if this is the next expected serial number
	chunkMeta.mutex.RLock()
	expectedSerial := chunkMeta.lastAppliedSerial + 1
	chunkMeta.mutex.RUnlock()

	if serialNumber == expectedSerial {
		// Apply this append immediately since it's the next in sequence
		err = applyAppend(chunkId, bufferMeta.data)
		if err != nil {
			return nil, err
		}

		// Clean up this buffer entry
		server.writeBuffers.Delete(bufferKey)

		// Update the last applied serial number
		chunkMeta.mutex.Lock()
		chunkMeta.lastAppliedSerial = serialNumber
		chunkMeta.mutex.Unlock()

		log.Printf("Secondary %s applied append with serial number %d to chunk %s",
			server.address, serialNumber, chunkId)

		// Check for any pending appends that can now be applied
		server.applyPendingAppends(chunkId, chunkMeta)
	} else if serialNumber > expectedSerial {
		// Store as pending append to apply later when all earlier appends arrive
		pendingKey := fmt.Sprintf("%s-%d", chunkId, serialNumber)
		server.pendingAppends.Store(pendingKey, &PendingAppend{
			data:         bufferMeta.data,
			serialNumber: serialNumber,
			applied:      false,
		})

		log.Printf("Secondary %s stored out-of-order append with serial number %d (expected %d)",
			server.address, serialNumber, expectedSerial)
	} else {
		// Ignore duplicate append with lower serial number
		log.Printf("Secondary %s ignoring duplicate append with serial number %d (already applied up to %d)",
			server.address, serialNumber, chunkMeta.lastAppliedSerial)
	}

	return &chunkserverpb.ForwardAppendRes{Success: true}, nil
}

// Helper function to apply an append to the local chunk
func applyAppend(chunkId string, data string) error {
	chunkPath := filepath.Join(chunksDirectory, chunkId)
	f, err := os.OpenFile(chunkPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to open chunk file: %v", err)
	}
	defer f.Close()

	// Write the data to the local chunk
	_, err = f.Write([]byte(data))
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to write data: %v", err)
	}

	return nil
}

// Helper function to apply any pending appends that are now ready
func (server *ChunkServer) applyPendingAppends(chunkId string, chunkMeta *ChunkMeta) {
	// Keep applying appends as long as we find the next serial number
	for nextSerial := chunkMeta.lastAppliedSerial + 1; ; nextSerial++ {
		pendingKey := fmt.Sprintf("%s-%d", chunkId, nextSerial)
		pendingInterface, exists := server.pendingAppends.Load(pendingKey)
		if !exists {
			// No more consecutive pending appends
			break
		}

		pending := pendingInterface.(*PendingAppend)
		if pending.applied {
			// This append was already applied
			server.pendingAppends.Delete(pendingKey)
			continue
		}

		// Apply this pending append
		err := applyAppend(chunkId, pending.data)
		if err != nil {
			log.Printf("Failed to apply pending append: %v", err)
			break
		}

		// Mark as applied and update chunk state
		pending.applied = true
		server.pendingAppends.Delete(pendingKey)
		chunkMeta.mutex.Lock()
		chunkMeta.lastAppliedSerial = nextSerial
		chunkMeta.mutex.Unlock()

		log.Printf("Secondary %s applied pending append with serial number %d to chunk %s",
			server.address, nextSerial, chunkId)
	}
}

// ReadDataRPC handles requests to read data from a chunk
func (server *ChunkServer) ReadDataRPC(ctx context.Context, req *chunkserverpb.ReadDataReq) (*chunkserverpb.ReadDataRes, error) {
	chunkId := req.ChunkId
	offset := req.Offset
	length := req.Length

	// Validate parameters
	if chunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "Chunk ID cannot be empty")
	}

	if offset < 0 {
		return nil, status.Error(codes.InvalidArgument, "Offset cannot be negative")
	}

	if length <= 0 {
		return nil, status.Error(codes.InvalidArgument, "Length must be positive")
	}

	// Check if this server has the requested chunk
	_, err := server.getChunkMeta(chunkId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Chunk %s not found on this server", chunkId)
	}

	// Open the chunk file
	chunkPath := filepath.Join(chunksDirectory, chunkId)
	file, err := os.Open(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "Chunk file %s does not exist", chunkId)
		}
		return nil, status.Errorf(codes.Internal, "Failed to open chunk file: %v", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get chunk file info: %v", err)
	}

	fileSize := fileInfo.Size()

	// Adjust length if it goes beyond file size
	if offset >= fileSize {
		return &chunkserverpb.ReadDataRes{
			Data:    "",
			Success: true,
		}, nil // Reading past end of file returns empty data
	}

	if offset+length > fileSize {
		length = fileSize - offset
	}

	// Read the data
	data := make([]byte, length)
	_, err = file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, status.Errorf(codes.Internal, "Failed to read data: %v", err)
	}

	return &chunkserverpb.ReadDataRes{
		Data:    string(data),
		Success: true,
	}, nil
}

func writeTofileAtOfsset(filePath, data string, offset int64) error {
	fmt.Printf("OFFSET:%d\n", offset)
	// Open file with read-write and create if not exists
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return status.Errorf(codes.Aborted, "Failed to open file: %v", err)
	}
	defer file.Close()

	// Write data at specific offset
	_, err = file.WriteAt([]byte(data), offset)
	if err != nil {
		return status.Errorf(codes.Aborted, "Failed to write at offset: %v", err)
	}
	return nil
}

func (server *ChunkServer) applyWrite(chunkId, clientAddr string) error {
	// Get chunk metadata
	chunkMeta, err := server.getChunkMeta(chunkId)
	if err != nil {
		return err
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	// Increase serial numer
	chunkMeta.serialNumber = chunkMeta.serialNumber + 1

	// Retrieve buffered write data
	bufferKey := fmt.Sprintf("%s-%s", chunkId, clientAddr)
	bufferVal, exists := server.writeBuffers.Load(bufferKey)
	if !exists {
		return status.Errorf(codes.NotFound,
			"No buffered data found for chunk %s from client %s secondary %s",
			chunkId, clientAddr, server.address)
	}
	buffer := bufferVal.(*WriteBufferMeta)

	// 3. Verify chunk version
	if buffer.version != chunkMeta.version {
		return status.Errorf(codes.Aborted,
			"Version mismatch: buffer version %d, chunk version %d",
			buffer.version, chunkMeta.version)
	}

	// Write to file
	filepath := filepath.Join(chunksDirectory, chunkId)
	if err := writeTofileAtOfsset(filepath, buffer.data, buffer.offset); err != nil {
		return err
	}

	// 6. clean buffer
	chunkMeta.lastModified = time.Now()
	// fmt.Printf("DELETE Chunkserver %s is deleting bufferkey %s\n", server.address, bufferKey)
	server.writeBuffers.Delete(bufferKey)

	return nil
}

func (server *ChunkServer) ForwardWriteRPC(ctx context.Context, req *chunkserverpb.ForwardWriteReq) (*chunkserverpb.ForwardWriteRes, error) {
	// Get chunk metadata
	chunkMeta, err := server.getChunkMeta(req.ChunkId)
	if err != nil {
		return nil, err
	}

	// Check if this is the next expected write
	chunkMeta.mutex.RLock()
	lastAppliedSerialNumber := chunkMeta.serialNumber
	chunkMeta.mutex.RUnlock()

	switch {
	case req.SerialNumber == lastAppliedSerialNumber+1:
		// Apply the write immediately if it's the next in sequence
		err := server.applyWrite(req.ChunkId, req.ClientAddress)
		if err != nil {
			return nil, err
		}
		return &chunkserverpb.ForwardWriteRes{Success: true}, nil

	case req.SerialNumber > lastAppliedSerialNumber+1:
		// If it's a future write, wait for previous writes to complete
		waitCtx, cancel := context.WithTimeout(ctx, PENDING_WRITE_WAIT_INTERVAL)
		defer cancel()

		ticker := time.NewTicker(PENDIND_WRITE_TICKER_INTERVAL)
		defer ticker.Stop()

		for {
			select {
			case <-waitCtx.Done():
				chunkMeta.mutex.RLock()
				currentSerial := chunkMeta.serialNumber
				chunkMeta.mutex.RUnlock()
				return nil, status.Errorf(codes.DeadlineExceeded,
					"Timed out waiting for previous writes to complete (current serial: %d, waiting for: %d)",
					currentSerial, req.SerialNumber)

			case <-ticker.C:
				chunkMeta.mutex.RLock()
				currentSerial := chunkMeta.serialNumber
				chunkMeta.mutex.RUnlock()

				switch {
				case req.SerialNumber == currentSerial+1:
					// Now we can apply the write
					err := server.applyWrite(req.ChunkId, req.ClientAddress)
					if err != nil {
						return nil, err
					}
					return &chunkserverpb.ForwardWriteRes{Success: true}, nil

				case req.SerialNumber <= currentSerial:
					// This write was already applied or is stale
					log.Printf("Write with serial %d is no longer needed (current serial is %d)",
						req.SerialNumber, currentSerial)
					return &chunkserverpb.ForwardWriteRes{Success: true}, nil
				}
				// Otherwise continue waiting
			}
		}

	default:
		// Ignore if it's a duplicate or old write
		log.Printf("Stale serial number: received %d, current %d. Ignoring this request\n",
			req.SerialNumber, lastAppliedSerialNumber)
		return &chunkserverpb.ForwardWriteRes{Success: true}, nil
	}
}

func (server *ChunkServer) CommitWriteRPC(ctx context.Context, req *chunkserverpb.CommitWriteReq) (*chunkserverpb.CommitWriteRes, error) {
	// 1. Get chunk metadata and verify primary status
	chunkMeta, err := server.getChunkMeta(req.ChunkId)
	if err != nil {
		return nil, err
	}

	chunkMeta.mutex.Lock()

	// Verify primary status and lease
	if !chunkMeta.isPrimary || time.Now().After(chunkMeta.leaseExpiry) {
		chunkMeta.mutex.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition,
			"Not primary or lease expired for chunk %s", req.ChunkId)
	}

	// 2. Retrieve buffered write data
	bufferKey := fmt.Sprintf("%s-%s", req.ChunkId, req.ClientAddress)
	// fmt.Printf("Buffered Key: %s\n", bufferKey)
	// server.printWriteBufferData()
	bufferVal, exists := server.writeBuffers.Load(bufferKey)
	if !exists {
		chunkMeta.mutex.Unlock()
		return nil, status.Errorf(codes.NotFound,
			"No buffered data found for chunk %s from client %s primary %s",
			req.ChunkId, req.ClientAddress, server.address)
	}
	buffer := bufferVal.(*WriteBufferMeta)

	// 3. Verify chunk version
	if buffer.version != chunkMeta.version {
		chunkMeta.mutex.Unlock()
		return nil, status.Errorf(codes.Aborted,
			"Version mismatch: buffer version %d, chunk version %d",
			buffer.version, chunkMeta.version)
	}

	// 4. Commit write to local disk
	filepath := filepath.Join(chunksDirectory, req.ChunkId)
	if err := writeTofileAtOfsset(filepath, buffer.data, buffer.offset); err != nil {
		chunkMeta.mutex.Unlock()
		return nil, err
	}

	// 5. Forward to secondaries using channels
	chunkMeta.serialNumber++
	chunkMeta.lastModified = time.Now()
	serialNumber := chunkMeta.serialNumber
	lenSecondaries := len(chunkMeta.secondaries)
	chunkMeta.mutex.Unlock() // release lock here to primary continues with another client request

	// Channel to collect results
	resultChan := make(chan struct {
		addr string
		err  error
	}, lenSecondaries)

	// Launch goroutines for each secondary
	for _, addr := range chunkMeta.secondaries {
		go func(addr string) {
			secondaryClient, err := getServerClient(addr)
			if err != nil {
				resultChan <- struct {
					addr string
					err  error
				}{addr, fmt.Errorf("connection failed: %v", err)}
				return
			}

			forwardCtx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
			defer cancel()

			_, err = secondaryClient.ForwardWriteRPC(forwardCtx, &chunkserverpb.ForwardWriteReq{
				ChunkId:       req.ChunkId,
				ClientAddress: req.ClientAddress,
				SerialNumber:  serialNumber,
			})

			resultChan <- struct {
				addr string
				err  error
			}{addr, err}
		}(addr)
	}

	// Collect results
	var failures []string
	for range chunkMeta.secondaries {
		res := <-resultChan
		if res.err != nil {
			failures = append(failures, fmt.Sprintf("- %s: %v", res.addr, res.err))
		}
	}

	// Clean up buffer
	// log.Printf("DELETE Chunkserver %s is deleting bufferkey %s\n", server.address, bufferKey)
	server.writeBuffers.Delete(bufferKey)

	// Prepare response
	if len(failures) > 0 {
		ErrorMessage := fmt.Sprintf("Write failed on %d secondaries:\n%s", len(failures), strings.Join(failures, "\n"))
		return nil, status.Errorf(codes.Canceled, "%s", ErrorMessage)
	}

	return &chunkserverpb.CommitWriteRes{Success: true}, nil
}

func (server *ChunkServer) printWriteBufferData() {
	fmt.Printf("Write Buffer Data:\n")
	server.writeBuffers.Range(func(key, value any) bool {
		bufferKey := key.(string)
		bufferMeta := value.(*WriteBufferMeta)
		fmt.Printf("bufferKey:%s data:%s \n", bufferKey, bufferMeta.data)
		return true
	})
}

// Store data in Buffer for write
func (server *ChunkServer) PushDataForWriteRPC(ctx context.Context, req *chunkserverpb.PushDataForWriteReq) (*chunkserverpb.PushDataForWriteRes, error) {
	// 1. Store data in buffer
	bufferKey := fmt.Sprintf("%s-%s", req.ChunkId, req.ClientAddr)
	server.writeBuffers.Store(bufferKey, &WriteBufferMeta{
		data:    req.Data,
		offset:  req.Offset,
		version: req.ChunkVersion,
	})

	// 2. If there are remaining replicas, forward the data
	if len(req.RemainingReplicas) > 0 {
		nextReplica := req.RemainingReplicas[0]
		nextClient, err := getServerClient(nextReplica)
		if err != nil {
			return nil, err
		}

		forwardReq := &chunkserverpb.PushDataForWriteReq{
			ChunkId:           req.ChunkId,
			Offset:            req.Offset,
			Data:              req.Data,
			ClientAddr:        req.ClientAddr,
			RemainingReplicas: req.RemainingReplicas[1:],
			ChunkVersion:      req.ChunkVersion,
		}

		res, err := nextClient.PushDataForWriteRPC(ctx, forwardReq)
		if err != nil || !res.Success {
			return nil, err
		}
	}

	// print write buffer data
	server.printWriteBufferData()

	return &chunkserverpb.PushDataForWriteRes{Success: true}, nil
}

// Update Chunk version rpc
func (server *ChunkServer) UpdateChunkVersionRPC(ctx context.Context, req *chunkserverpb.UpdateChunkVersionReq) (*chunkserverpb.UpdateChunkVersionRes, error) {
	chunkMeta, err := server.getChunkMeta(req.ChunkId)
	if err != nil {
		return nil, err
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	chunkMeta.version++
	chunkMeta.lastModified = time.Now()

	return &chunkserverpb.UpdateChunkVersionRes{Success: true}, nil
}

// Granting lease rpc
func (server *ChunkServer) GrantLeaseRPC(ctx context.Context, req *chunkserverpb.GrantLeaseReq) (*chunkserverpb.GrantLeaseRes, error) {
	now := time.Now()
	chunkMeta, err := server.getChunkMeta(req.ChunkId)
	if err != nil {
		return nil, err
	}

	chunkMeta.mutex.Lock()
	defer chunkMeta.mutex.Unlock()

	chunkMeta.isPrimary = true
	chunkMeta.version = req.Version
	chunkMeta.leaseExpiry = now.Add(time.Duration(req.LeaseTime) * time.Second)

	chunkMeta.secondaries = make([]string, 0)
	chunkMeta.secondaries = append(chunkMeta.secondaries, req.Secondaries...)

	return &chunkserverpb.GrantLeaseRes{Success: true}, nil
}

// Assign chunk
func (server *ChunkServer) AssignChunkRPC(ctx context.Context, req *chunkserverpb.AssignChunkReq) (*chunkserverpb.AssignChunkRes, error) {
	now := time.Now()
	server.chunks.Store(req.ChunkId, &ChunkMeta{
		version:           req.Version,
		isPrimary:         false,
		lastModified:      now,
		serialNumber:      0,
		nextSerialNumber:  0,
		lastAppliedSerial: -1,
	})
	return &chunkserverpb.AssignChunkRes{Success: true}, nil
}

// Get free disk space of directory of chunkserver
func getDiskSpace() (total, free uint64) {
	usage, err := disk.Usage(chunksDirectory)
	if err != nil {
		log.Printf("Failed to get disk usage for chunkserver %s: %v\n", port, err)
		return 0, 0
	}
	return usage.Total, usage.Free
}

// Chunkserver sends heartbeat periodically to master
func (server *ChunkServer) sendHeartbeat() {
	consecutiveFailures := 0

	for {
		var chunks []*masterpb.Chunk
		server.chunks.Range(func(key, value interface{}) bool {
			chunkId := key.(string)
			chunkMeta := value.(*ChunkMeta)

			chunkMeta.mutex.RLock()
			defer chunkMeta.mutex.RUnlock()

			// // Check if chunk file exists on disk
			// chunkPath := filepath.Join(chunksDirectory, chunkId)
			// _, err := os.Stat(chunkPath)
			// hasError := false // Default to no error
			// if err != nil {
			// 	if os.IsNotExist(err) {
			// 		// File definitely doesn't exist
			// 		fmt.Printf("FILE_NOT_FOUND:%v\n",err)
			// 		hasError = true
			// 	} else {
			// 		// Some other error occurred, but we'll ignore it as requested
			// 		fmt.Printf("Warning: Error accessing chunk %s: %v (ignoring as requested)\n", chunkId, err)
			// 	}
			// }

			chunks = append(chunks, &masterpb.Chunk{
				ChunkId:  chunkId,
				Version:  chunkMeta.version,
				HasError: false,
			})
			return true
		})

		_, freeSpace := getDiskSpace()

		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
		_, err := masterClient.HeartbeatRPC(ctx, &masterpb.HeartbeatReq{
			Address:   server.address,
			Chunks:    chunks,
			DiskSpace: freeSpace,
		})
		cancel()

		if err != nil {
			log.Printf("Failed to send heartbeat: %v\n", err)
			consecutiveFailures++

			// After several failures, try another master
			if consecutiveFailures >= 3 {
				log.Printf("Master may be down, attempting to connect to another master")
				// Move to the next master before reconnecting
				currentMasterIndex = (currentMasterIndex + 1) % len(masterAddresses)
				connectToMaster()
				consecutiveFailures = 0
			}
		} else {
			consecutiveFailures = 0
		}

		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

// //////////// Chunk server setup functions ///////////////////////////////////////////

func (server *ChunkServer) ExitRPC(ctx context.Context, req *chunkserverpb.Empty) (*chunkserverpb.Empty, error) {
	os.Exit(0)
	return nil, nil
}

// Helper function to extract method name from full method path
func extractMethodName(fullMethod string) string {
	parts := strings.Split(fullMethod, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return fullMethod
}

// Log request and responces of Chunk server RPC calls
func loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	methodName := extractMethodName(info.FullMethod)
	var logRequest, logResponse string
	logRequest = fmt.Sprintf("Methodname: %s | Request: %v\n", methodName, req)
	log.Printf("→ [ChunkServer Request] %s", logRequest)
	res, err := handler(ctx, req)
	// Capture error and response details
	if err != nil {
		st, _ := status.FromError(err)
		logResponse = fmt.Sprintf("Methodname: %s | status: Fail | ErrorCode: %v| ErrorMessage: %v", methodName, st.Code(), st.Message())
	} else {
		logResponse = fmt.Sprintf("Methodname: %s | status: Success | Response: %v\n", methodName, res)
	}

	log.Printf("← [ChunkServer Response] %s", logResponse)
	return res, err
}

func connectToMaster() {
    fmt.Println("Client attempting to connect to a master...")
    
    // First try to get master from etcd
    masterAddr, err := getMasterFromEtcd()
    if err == nil {
        // Found master in etcd, try to connect
        conn, err := grpc.Dial("localhost:"+masterAddr, grpc.WithInsecure())
        if err == nil {
            client := masterpb.NewMasterServiceClient(conn)
            
            // Test the connection
            ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
            _, testErr := client.HeartbeatRPC(ctx, &masterpb.HeartbeatReq{Address: port})
            cancel()
            
            if testErr == nil {
                // Connection is working
                masterClient = client
                currentMasterIndex = findMasterIndex(masterAddr)
                log.Printf("Successfully connected to master at %s (from etcd)", masterAddr)
                return
            }
            
            conn.Close()
            log.Printf("Master from etcd at %s not responding: %v", masterAddr, testErr)
        }
    }
    
    // If etcd failed or master is not responsive, fall back to the list-based approach
    fmt.Printf("Etcd-based master discovery failed, falling back to list: %v\n", masterAddresses)
    
    // Try each master address in the list
    startIndex := currentMasterIndex
    for i := 0; i < len(masterAddresses); i++ {
        index := (startIndex + i) % len(masterAddresses)
        addr := masterAddresses[index]
        
        fmt.Printf("Trying to connect to master at %s\n", addr)
        
        // Try to connect
        conn, err := grpc.Dial("localhost:"+addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(3*time.Second))
        if err != nil {
            fmt.Printf("Failed to connect to master at %s: %v\n", addr, err)
            continue // Try the next master
        }
        
        // Success
        masterClient = masterpb.NewMasterServiceClient(conn)
        currentMasterIndex = index
        fmt.Printf("Successfully connected to master at %s\n", addr)
        return
    }
    
    // If we get here, we couldn't connect to any master
    log.Fatalf("Failed to connect to any master server. Addresses tried: %v", masterAddresses)
}

// create tcp listener on allocated port
func createTCPListener() net.Listener {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v\n", err)
	}
	return listener
}

// create directory to store chunk files for chunkServer
func setupChunksDirectory(serverAddr string) {
	if err := os.MkdirAll(dataBasepath, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v\n", err)
	}

	chunksDirectory = filepath.Join(dataBasepath, serverAddr)
	if err := os.MkdirAll(chunksDirectory, 0755); err != nil {
		log.Fatalf("Failed to create chunk server directory: %v\n", err)
	}
}

func init() {
	var masterAddrList string
	flag.StringVar(&masterAddrList, "masterAddrs", "50050,50060", "Comma-separated list of master addresses")
	flag.StringVar(&port, "port", "500XY", "Unique port of the chunk server")
	flag.StringVar(&masterAddr, "masterAddr", "50050", "Address of Master server")
	flag.Parse()

	// Print flags
	// fmt.Printf("Port: %s, masterAddr: %s\n",port, masterAddr)

	masterAddresses = strings.Split(masterAddrList, ",")
	currentMasterIndex = 0

	setupChunksDirectory(port)
}

////////////// END of Chunk server setup functions ////////////////////////////////////

func main() {
	listener := createTCPListener()
	defer listener.Close()

	// Initialize etcd client
    if err := initEtcdClient(); err != nil {
        log.Printf("Warning: Failed to initialize etcd client: %v", err)
        log.Printf("Falling back to master list-based discovery")
    } else {
        // Start watching for master changes
        go watchMasterChanges()
    }

	chunkServer := NewChunkServer(port)
	connectToMaster()

	// Create a grpc server and register chunk server with chunkserverService
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingInterceptor),
	)
	chunkserverpb.RegisterChunkserverServiceServer(grpcServer, chunkServer)

	go chunkServer.sendHeartbeat()

	fmt.Println("Chunk Server is listening on port", port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v\n", err)
	}
}
