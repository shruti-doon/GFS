package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	chunkserverpb "github.com/laharisiri123/Distributed-Google-File-System-Project/protofiles/chunkserver"
	masterpb "github.com/laharisiri123/Distributed-Google-File-System-Project/protofiles/master"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	masterAddr         string
	masterClient       masterpb.MasterServiceClient
	masterAddresses    []string
	currentMasterIndex int
	queriesfile        = ""
)

const (
	RPC_TIMEOUT_INTERVAL   = time.Second * 10
	CHUNK_SIZE             = 64 * 1000000
	FILE_DELETION_INTERVAL = time.Second * 5 // Delete requested files will be deleted if this period is exceeded
)

type WriteChunk struct {
	chunkIndex int64
	offset     int64
	data       string
}

// AppendChunk represents a chunk of data for appending
type AppendChunk struct {
	data string
}

// Command struct for parsing JSON
type Query struct {
	Command       string `json:"command"`
	ClientAddress string `json:"client_addr,omitempty"`
	FileName      string `json:"file_name,omitempty"`
	DirectoryPath string `json:"directory_path,omitempty"`
	Offset        int64  `json:"offset,omitempty"`
	DataFileName  string `json:"data_file_name,omitempty"`
	SleepTime     int    `json:"duration,omitempty"`
}

type QueryList struct {
	Queries []Query `json:"queries"`
}

// /////////////////// Client Operations ////////////////////////////////////////////////
func getServerClient(address string) (chunkserverpb.ChunkserverServiceClient, error) {
	// Establish connection to the gRPC server
	conn, err := grpc.NewClient("localhost:"+address, grpc.WithInsecure())
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "Failed to connect to chunkserver %s: %v", address, err)
	}

	client := chunkserverpb.NewChunkserverServiceClient(conn)
	return client, nil
}

// retryWithMasterFailover attempts an operation and handles master failover if needed
func retryWithMasterFailover(operation func() error) error {
	maxRetries := 3

	for retry := 0; retry < maxRetries; retry++ {
		err := operation()

		// If successful, return immediately
		if err == nil {
			return nil
		}

		// Check if this is a connection error (master might be down)
		if strings.Contains(err.Error(), "connection refused") ||
			strings.Contains(err.Error(), "unavailable") {

			fmt.Printf("Connection to master failed: %v\n", err)

			// Try to connect to a different master
			currentMasterIndex = (currentMasterIndex + 1) % len(masterAddresses)
			fmt.Printf("Attempting to connect to next master at index %d\n", currentMasterIndex)
			connectToMaster()

			// Continue the retry loop with the new master
			continue
		}

		// If it's not a connection error, return the error immediately
		return err
	}

	return fmt.Errorf("operation failed after %d retries", maxRetries)
}

// getChunkDetailsForAppend gets chunk details from master for append operation
func getChunkDetailsForAppend(filePath string) (*masterpb.AppendRes, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()

	res, err := masterClient.GetChunkDetailsForAppendRPC(ctx, &masterpb.AppendReq{FilePath: filePath})
	return res, err
}

// commitAppend sends a commit message to the primary for an append operation
func commitAppend(primary string, chunkId string, clientAddr string) (bool, int64, error) {
	primaryClient, err := getServerClient(primary)
	if err != nil {
		return false, -1, fmt.Errorf("failed to connect to primary %s: %v", primary, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()

	res, err := primaryClient.CommitAppendRPC(ctx, &chunkserverpb.CommitAppendReq{
		ChunkId:       chunkId,
		ClientAddress: clientAddr,
	})

	if err != nil {
		return false, -1, err
	}

	return res.Success, res.Offset, nil
}

// appendToFile handles a single append operation to the specified file
func appendToFile(filePath string, data string, clientAddr string) error {
	// Split data into chunks for appending
	// Maximum append size is 1/4 of chunk size (64MB) = 16MB as per GFS paper
	maxAppendSize := CHUNK_SIZE / 4 // 16MB

	dataSize := len(data)
	var appendChunks []AppendChunk

	// If data is larger than maxAppendSize, split it
	for i := 0; i < dataSize; i += maxAppendSize {
		endOffset := min(dataSize, i+maxAppendSize)
		chunkData := data[i:endOffset]
		appendChunks = append(appendChunks, AppendChunk{data: chunkData})
	}

	// Process each chunk for appending
	for _, chunk := range appendChunks {
		// Get chunk details from master for append
		chunkDetails, err := getChunkDetailsForAppend(filePath)
		if err != nil {
			log.Printf("Failed to get chunk details for append: %v", err)
			return err
		}

		// Extract chunk information
		chunkId := chunkDetails.ChunkId
		chunkVersion := chunkDetails.ChunkVersion
		primary := chunkDetails.PrimaryAddress
		replicas := chunkDetails.Replicas

		fmt.Printf("Chunk details:\n")
		fmt.Printf("ChunkId:%s Version:%d Primary:%s Replicas:%v\n", chunkId, chunkVersion, primary, replicas)

		// Push data to replicas with offset as -1
		fmt.Printf("Pushing data to replicas...\n")
		pushStatus, err := pushDataToReplicas(chunkId, -1, chunk.data, clientAddr, chunkVersion, replicas)
		if err != nil || !pushStatus {
			log.Printf("Failed to push data to replicas for append: %v", err)
			return fmt.Errorf("failed to push data to replicas: %v", err)
		}

		// Send commit append message to primary
		fmt.Printf("Committing append to primary %s...\n", primary)
		commitStatus, offset, err := commitAppend(primary, chunkId, clientAddr)
		if err != nil || !commitStatus {
			log.Printf("Failed to commit append: %v", err)
			return fmt.Errorf("failed to commit append: %v", err)
		}

		// Calculate global file offset (chunkIndex * chunkSize + offset within chunk)
		chunkIndex := chunkDetails.ChunkIndex // You may need to add this field to your AppendRes
		globalOffset := int64(chunkIndex)*CHUNK_SIZE + offset

		fmt.Printf("Append successful at global offset %d (chunk %d, offset %d)\n",
			globalOffset, chunkIndex, offset)
	}

	return nil
}

// appendWithRetry attempts to append data to a file with retries
func appendWithRetry(filePath string, data string, clientAddr string, maxRetries int) error {
	retryCount := 0
	for retryCount < maxRetries {
		err := appendToFile(filePath, data, clientAddr)
		if err == nil {
			return nil // Success
		}

		log.Printf("Append retry %d failed: %v", retryCount, err)
		retryCount++
		// Add a small backoff delay
		time.Sleep(time.Duration(100*retryCount) * time.Millisecond)
	}

	return fmt.Errorf("failed to append to file after %d retries", maxRetries)
}

// Function to get chunk details from master
func getChunkDetailsForRead(filePath string, chunkIndex int64) (*masterpb.ReadRes, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()

	res, err := masterClient.GetChunkDetailsForReadRPC(ctx, &masterpb.ReadReq{
		FilePath:   filePath,
		ChunkIndex: chunkIndex,
	})

	return res, err
}

// Function to read data from a chunk
func readDataFromChunk(location string, chunkId string, offset int64, length int64) (string, error) {
	// Connect to the chunk server
	chunkServerClient, err := getServerClient(location)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()

	// Send read request
	res, err := chunkServerClient.ReadDataRPC(ctx, &chunkserverpb.ReadDataReq{
		ChunkId: chunkId,
		Offset:  offset,
		Length:  length,
	})

	if err != nil {
		return "", err
	}

	if !res.Success {
		return "", fmt.Errorf("read operation failed")
	}

	return res.Data, nil
}

// Read a specific chunk at offset with specified length
func readFromChunk(filePath string, chunkIndex int64, offset int64, length int64) (string, error) {
	var data string

	err := retryWithMasterFailover(func() error {
		// Get chunk details from master
		chunkDetails, err := getChunkDetailsForRead(filePath, chunkIndex)
		if err != nil {
			return fmt.Errorf("failed to get chunk details: %v", err)
		}

		// Select a replica (randomly or using a policy like closest)
		locations := chunkDetails.Locations
		if len(locations) == 0 {
			return fmt.Errorf("no locations available for chunk")
		}

		// Try each location until one succeeds
		var readErr error
		for _, location := range locations {
			data, readErr = readDataFromChunk(location, chunkDetails.ChunkId, offset, length)
			if readErr == nil {
				// Success - we have the data
				return nil
			}
		}

		// If we get here, all locations failed
		return fmt.Errorf("failed to read data from all replicas: %v", readErr)
	})

	if err != nil {
		return "", err
	}

	return data, nil
}

// Read the entire file
func readEntireFile(filePath string) (string, error) {
	var result strings.Builder
	var err error

	err = retryWithMasterFailover(func() error {
		result.Reset() // Clear any partial results
		chunkIndex := int64(0)

		for {
			// Get chunk details for this chunk index
			_, err := getChunkDetailsForRead(filePath, chunkIndex)
			if err != nil {
				if status.Code(err) == codes.OutOfRange {
					// We've read all chunks
					return nil
				}
				return fmt.Errorf("failed to get chunk details for index %d: %v", chunkIndex, err)
			}

			// Read the entire chunk (up to CHUNK_SIZE)
			data, err := readFromChunk(filePath, chunkIndex, 0, CHUNK_SIZE)
			if err != nil {
				return fmt.Errorf("failed to read chunk at index %d: %v", chunkIndex, err)
			}

			// Append the data to the result
			result.WriteString(data)

			// Move to the next chunk
			chunkIndex++
		}
	})

	if err != nil {
		return "", err
	}

	return result.String(), nil
}

// Read a portion of a file starting at specified offset with specified length
func readFileRange(filePath string, offset int64, length int64) (string, error) {
	var result strings.Builder

	err := retryWithMasterFailover(func() error {
		result.Reset() // Clear any partial results

		if offset < 0 {
			return fmt.Errorf("offset cannot be negative")
		}

		if length <= 0 {
			return fmt.Errorf("length must be positive")
		}

		// Calculate starting chunk index and offset within that chunk
		startChunkIndex := offset / CHUNK_SIZE
		startChunkOffset := offset % CHUNK_SIZE

		remainingLength := length
		currentChunkIndex := startChunkIndex
		currentOffset := startChunkOffset

		for remainingLength > 0 {
			// Calculate how much to read from this chunk
			lengthToRead := CHUNK_SIZE - currentOffset
			if lengthToRead > remainingLength {
				lengthToRead = remainingLength
			}

			// Read from this chunk
			data, err := readFromChunk(filePath, currentChunkIndex, currentOffset, lengthToRead)
			if err != nil {
				if status.Code(err) == codes.OutOfRange {
					// We've reached the end of the file
					break
				}
				return fmt.Errorf("failed to read chunk at index %d: %v", currentChunkIndex, err)
			}

			// If we got less data than requested, we've reached the end of the file or chunk
			if int64(len(data)) < lengthToRead {
				result.WriteString(data)
				break
			}

			// Append the data to the result
			result.WriteString(data)

			// Update remaining length
			remainingLength -= int64(len(data))

			// Move to the next chunk if necessary
			currentChunkIndex++
			currentOffset = 0
		}

		return nil
	})

	if err != nil {
		return "", err
	}

	return result.String(), nil
}

func splitDataIntoChunks(data string, fileOffset int64) []WriteChunk {
	var chunks []WriteChunk
	dataBytes := []byte(data)
	remainingData := dataBytes

	// Calculate initial chunk index and offset within that chunk
	chunkIndex := fileOffset / CHUNK_SIZE
	chunkOffset := fileOffset % CHUNK_SIZE

	for len(remainingData) > 0 {
		// Calculate available space in current chunk
		availableInChunk := CHUNK_SIZE - chunkOffset
		chunkDataSize := min(availableInChunk, int64(len(remainingData)))

		// Get data for this chunk
		chunkData := remainingData[:chunkDataSize]
		remainingData = remainingData[chunkDataSize:]

		// Create chunk
		chunks = append(chunks, WriteChunk{
			chunkIndex: chunkIndex,
			offset:     chunkOffset,
			data:       string(chunkData),
		})

		// Move to next chunk (offset is 0 for subsequent chunks)
		chunkIndex++
		chunkOffset = 0
	}

	fmt.Printf("Data split into chunks: \n")
	for i, chunk := range chunks {
		fmt.Printf("Chunk %d: | ChunkIndex: %d | offset: %d | data: %s\n",
			i, chunk.chunkIndex, chunk.offset, chunk.data)
	}

	return chunks
}

func getChunkDetailsForWrite(filePath string, chunkInd int64) (*masterpb.WriteRes, error) {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()
	res, err := masterClient.GetChunkDetailsForWriteRPC(ctx, &masterpb.WriteReq{FilePath: filePath, ChunkIndex: chunkInd})
	return res, err
}

func pushDataToReplicas(chunkId string, offset int64, data string, clientAddr string, chunkVersion int64, replicas []string) (bool, error) {
	replicasInt := make([]int, 0)
	for _, addr := range replicas {
		addrInt, _ := strconv.Atoi(addr)
		replicasInt = append(replicasInt, addrInt)
	}
	sort.Ints(replicasInt)
	replicasSorted := make([]string, 0)
	for _, addr := range replicasInt {
		addrStr := strconv.Itoa(addr)
		replicasSorted = append(replicasSorted, addrStr)
	}

	nearChunkServerClient, _ := getServerClient(replicasSorted[0])
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()
	res, err := nearChunkServerClient.PushDataForWriteRPC(ctx, &chunkserverpb.PushDataForWriteReq{
		ChunkId:           chunkId,
		Offset:            offset,
		Data:              data,
		ClientAddr:        clientAddr,
		RemainingReplicas: replicasSorted[1:],
		ChunkVersion:      chunkVersion,
	})

	if err != nil || !res.Success {
		return false, err
	}
	return true, err
}

func commitWrite(primary string, chunkId string, clientAddr string) (bool, error) {
	primaryClient, _ := getServerClient(primary)
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()
	res, err := primaryClient.CommitWriteRPC(ctx, &chunkserverpb.CommitWriteReq{ChunkId: chunkId,
		ClientAddress: clientAddr})
	if err != nil || !res.Success {
		return false, err
	}
	return true, err
}

func writeTofile(filePath string, offset int64, data string, clientAddr string) {
	chunks := splitDataIntoChunks(data, offset)
	for i, chunk := range chunks {
		fmt.Printf("Trying to write chunk %d\n", i)
		// 1. Get chunk details from master

		res, err := getChunkDetailsForWrite(filePath, chunk.chunkIndex)
		if err != nil {
			log.Printf("Failed while getting chunkdetails for write in file %s at offset %d: %v", filePath, offset, err)
			return
		}

		chunkId := res.ChunkId
		chunkVersion := res.ChunkVersion
		primary := res.PrimaryAddress
		replicas := res.Replicas
		fmt.Printf("Chunk details:\n")
		fmt.Printf("ChunkId:%s Version:%d Primary:%s Replicas:%v\n", chunkId, chunkVersion, primary, replicas)

		// 2. Push data to replicas in pipelined manner
		pushStatus, err := pushDataToReplicas(chunkId, chunk.offset, chunk.data, clientAddr, chunkVersion, replicas)
		if err != nil || !pushStatus {
			log.Printf("Failed while pushing data to  chunkserver for write in file %s at offset %d: %v\n", filePath, offset, err)
			return
		}

		// Send commit write message to primary
		commitStatus, err := commitWrite(primary, chunkId, clientAddr)
		if err != nil || !commitStatus {
			log.Printf("Failed while commiting write data to  chunkserver for write in file %s at offset %d: %v\n", filePath, offset, err)
			return
		}

		fmt.Printf("successfully written to all chunkservers\n")
	}
}

// UndeleteFile recovers a deleted file
func undeleteFile(filePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()

	res, err := masterClient.UndeleteFileRPC(ctx, &masterpb.UndeleteFileReq{
		FilePath: filePath,
	})

	if err != nil {
		return fmt.Errorf("failed to undelete file: %v", err)
	}

	if !res.Success {
		return fmt.Errorf("file undeletion failed")
	}

	return nil
}

// DeleteFile deletes a file from the filesystem
func deleteFile(filePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	defer cancel()

	res, err := masterClient.DeleteFileRPC(ctx, &masterpb.DeleteFileReq{
		FilePath: filePath,
	})

	if err != nil {
		return fmt.Errorf("failed to delete file: %v", err)
	}

	if !res.Success {
		return fmt.Errorf("file deletion failed")
	}

	return nil
}

func createFile(filepath string) {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	res, err := masterClient.CreateFileRPC(ctx, &masterpb.CreateFileReq{
		FilePath: filepath,
	})
	defer cancel()

	if err != nil {
		log.Printf("Failed to create file: %v\n", err)
		return
	}

	if res.Success {
		fmt.Printf("%s file created successfully\n", filepath)
	}
}

func createDirectory(dirPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	res, err := masterClient.CreateDirectoryRPC(ctx, &masterpb.CreateDirectoryReq{
		DirectoryPath: dirPath,
	})
	cancel()

	if err != nil {
		log.Printf("Failed to create directory: %v\n", err)
		return
	}

	if res.Success {
		fmt.Printf("%s directory created successfully\n", dirPath)
	}
}

///////////////////// END of Client Operations ////////////////////////////////////////////////

///////////////////// Client setup functions ///////////////////////////////////////////

// connect to master with failover support
func connectToMaster() {
	fmt.Println("Client attempting to connect to a master...")

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

////////////////////// Quries Execution ////////////////////////////////////

func executeQuries() {
	queries := getQueryList(queriesfile)

	for _, q := range queries {
		switch q.Command {
		case "create_directory":
			createDirectory(q.DirectoryPath)
		case "create_file":
			createFile(q.FileName)
		case "append":
			data := readDataForAppendOrWrite(q.DataFileName)
			appendWithRetry(q.FileName, data, q.ClientAddress, 3)
		case "sleep":
			time.Sleep(time.Duration(q.SleepTime) * time.Second)
		default:
			log.Printf("Unknown command: %s", q.Command)
		}
		// Small delay between commands for better logging readability
		time.Sleep(100 * time.Millisecond)
	}
}

func getQueryList(filename string) []Query {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read queries JSON file: %v", err)
	}

	var queries []Query
	err = json.Unmarshal(data, &queries)
	if err != nil {
		log.Fatalf("Failed to parse query file JSON: %v", err)
	}

	return queries
}

func readDataForAppendOrWrite(filename string) string {

	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	return string(data)
}

////////////////////// END of Quries Execution ////////////////////////////////////

func init() {
	var masterAddrList string
	flag.StringVar(&masterAddrList, "masterAddrs", "50050,50060", "Comma-separated list of master addresses")
	flag.StringVar(&masterAddr, "masterAddr", "50050", "Address of master server")
	flag.StringVar(&queriesfile, "query_file", "features_queries/append", "query file for feature demo")

	flag.Parse()

	// Use masterAddrList if provided, otherwise use the single masterAddr
	if masterAddrList != "" {
		masterAddresses = strings.Split(masterAddrList, ",")
	} else {
		masterAddresses = []string{masterAddr}
	}

	fmt.Printf("Client will try to connect to masters: %v\n", masterAddresses)
	currentMasterIndex = 0
}

///////////////////// END of Client setup functions ////////////////////////////////////

func main() {

	connectToMaster()

	executeQuries()

	// createDirectory("/dir1")
	// // createFile("/dir1")
	// // go createFile("/dir1/file1.txt")
	// createFile("/dir1/file1.txt")
	// // writeTofile("/dir1/file1.txt", 0, "We are implementing Distribute Google File System.")
	// go writeTofile("/dir1/file1.txt", 0, "Distributed Google Filesystem", "1")

	// go appendWithRetry("/dir1/file1.txt", " This is an append to the existing file.", "2", 3)
	// go appendWithRetry("/dir1/file1.txt", " Appending more text to test read functionality.", "3", 3)

	// time.Sleep(time.Second * 5)
	// // Read the entire file
	// entireFileContent, err := readEntireFile("/dir1/file1.txt")
	// if err != nil {
	// 	log.Printf("Failed to read entire file: %v", err)
	// } else {
	// 	fmt.Printf("Entire file content:\n%s\n", entireFileContent)
	// }

	// // Read a specific range
	// rangeContent, err := readFileRange("/dir1/file1.txt", 10, 20)
	// if err != nil {
	// 	log.Printf("Failed to read file range: %v", err)
	// } else {
	// 	fmt.Printf("Content from offset 10, length 20:\n%s\n", rangeContent)
	// }

	// // Read a specific chunk
	// chunkContent, err := readFromChunk("/dir1/file1.txt", 0, 5, 15)
	// if err != nil {
	// 	log.Printf("Failed to read from chunk: %v", err)
	// } else {
	// 	fmt.Printf("Chunk content from offset 5, length 15:\n%s\n", chunkContent)
	// }

	// // Checking delete file

	// connectToMaster()

	// // Create directory and file
	// createDirectory("/dir1")
	// createFile("/dir1/file1.txt")

	// // Write some data
	// writeTofile("/dir1/file1.txt", 0, "Distributed Google Filesystem", "1")
	// appendWithRetry("/dir1/file1.txt", " This is an append to the existing file.", "2", 3)
	// appendWithRetry("/dir1/file1.txt", " Appending more text to test read functionality.", "3", 3)

	// // Read before deletion
	// fmt.Println("\nReading file before deletion:")
	// content, err := readEntireFile("/dir1/file1.txt")
	// if err != nil {
	// 	log.Printf("Failed to read file: %v", err)
	// } else {
	// 	fmt.Printf("File content: %s\n", content)
	// }

	// // Delete the file
	// fmt.Println("\nDeleting file...")
	// err = deleteFile("/dir1/file1.txt")
	// if err != nil {
	// 	log.Printf("Failed to delete file: %v", err)
	// } else {
	// 	fmt.Println("File deleted successfully (marked for deletion)")
	// }

	// // Try to read immediately after deletion (should still work during grace period)
	// fmt.Println("\nReading immediately after deletion (should still work):")
	// content, err = readEntireFile("/dir1/file1.txthidden")
	// if err != nil {
	// 	log.Printf("Failed to read file: %v", err)
	// } else {
	// 	fmt.Printf("File content: %s\n", content)
	// }

	// // Undelete the file
	// fmt.Println("\nUndeleting file...")
	// err = undeleteFile("/dir1/file1.txt")
	// if err != nil {
	// 	log.Printf("Failed to undelete file: %v", err)
	// } else {
	// 	fmt.Println("File undeleted successfully")
	// }

	// // Read after undeletion
	// fmt.Println("\nReading after undeletion:")
	// content, err = readEntireFile("/dir1/file1.txt")
	// if err != nil {
	// 	log.Printf("Failed to read file: %v", err)
	// } else {
	// 	fmt.Printf("File content: %s\n", content)
	// }

	// // Delete again and wait past grace period
	// fmt.Println("\nDeleting file again...")
	// err = deleteFile("/dir1/file1.txt")
	// if err != nil {
	// 	log.Printf("Failed to delete file: %v", err)
	// } else {
	// 	fmt.Println("File deleted successfully (marked for deletion)")
	// }

	// // Simulate waiting past the grace period (FILE_DELETION_INTERVAL)
	// fmt.Printf("\nWaiting for grace period (%v)...\n", FILE_DELETION_INTERVAL)
	// time.Sleep(FILE_DELETION_INTERVAL + time.Second)

	// // Try to read after grace period (should fail)
	// fmt.Println("\nReading after grace period (should fail):")
	// content, err = readEntireFile("/dir1/file1.txthidden")
	// if err != nil {
	// 	log.Printf("Failed to read file (expected after grace period): %v", err)
	// } else {
	// 	fmt.Printf("File content: %s\n", content)

	// // Checking rereplication
	// createDirectory("/dir1")
	// createFile("/dir1/file1.txt")
	// writeTofile("/dir1/file1.txt", 0, "We are implementing Distribute Google File System.", "1")
	// chunkDetails, err := getChunkDetailsForRead("/dir1/file1.txt", 0)
	// if err != nil {
	// 	fmt.Printf("Error while getting chunk details: %v\n", err)
	// 	return
	// }

	// if len(chunkDetails.Locations) == 0 {
	// 	fmt.Printf("No chunkServers available for this chunk\n")
	// 	return
	// }

	// fmt.Printf("Current ChunkServers: ")
	// for _, addr := range chunkDetails.Locations{
	// 	fmt.Printf("%s ",addr)
	// }
	// fmt.Println()
	// // take some chunk server and simulate it to fail and the try after some interval to check whether rerepliation happened or not

	// chunkServer := chunkDetails.Locations[0]
	// client, err := getServerClient(chunkServer)
	// if err != nil{
	// 	fmt.Printf("%v\n",err)
	// }
	// ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT_INTERVAL)
	// defer cancel()
	// client.ExitRPC(ctx,&chunkserverpb.Empty{})

	// time.Sleep(time.Second*5)

	// chunkDetails, err = getChunkDetailsForRead("/dir1/file1.txt", 0)
	// if err != nil {
	// 	fmt.Printf("Error while getting chunk details: %v\n", err)
	// 	return
	// }

	// if len(chunkDetails.Locations) == 0 {
	// 	fmt.Printf("No chunkServers available for this chunk\n")
	// 	return
	// }

	// fmt.Printf("Current ChunkServers: ")
	// for _, addr := range chunkDetails.Locations{
	// 	fmt.Printf("%s ",addr)
	// }
	// fmt.Println()
}