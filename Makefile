PROTO_DIR = protofiles
CLIENT_DIR = client
CHUNK_SERVER_DIR = server/chunkServer
MASTER_SERVER_DIR = server/masterServer
LOGS_DIR = logs

# Define separate output directories for different proto packages
MASTER_PROTO_DIR = $(PROTO_DIR)/master
CHUNK_SERVER_PROTO_DIR = $(PROTO_DIR)/chunkserver

# Define paths to .proto files
PROTO_FILES_MASTER = $(MASTER_PROTO_DIR)/master.proto
PROTO_FILES_CHUNK_SERVER = $(CHUNK_SERVER_PROTO_DIR)/chunkserver.proto

GO_FLAGS = --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative

NUM_CHUNK_SERVERS ?= 10
BASE_PORT ?= 50050
SHADOW_PORT ?= 50070

.PHONY: proto server client clean shadow-master

proto:
	protoc $(GO_FLAGS) $(PROTO_FILES_MASTER)
	protoc $(GO_FLAGS) $(PROTO_FILES_CHUNK_SERVER)

master-server:
	go run $(MASTER_SERVER_DIR)/main.go -N=$(NUM_CHUNK_SERVERS) -basePort=$(BASE_PORT)

shadow-master:
	go run $(MASTER_SERVER_DIR)/main.go -shadow=true -primary=$(BASE_PORT) -basePort=$(SHADOW_PORT)

chunk-servers:
	cd $(CHUNK_SERVER_DIR) && ./start_chunk_servers.sh $(NUM_CHUNK_SERVERS) $(BASE_PORT) "$(BASE_PORT),$(SHADOW_PORT)"

client:
	cd $(CLIENT_DIR) && go run main.go -masterAddrs="$(BASE_PORT),$(SHADOW_PORT)" -query_file="features_queries/append.json"

clean:
	rm -f $(MASTER_PROTO_DIR)/*pb.go $(CHUNK_SERVER_PROTO_DIR)/*pb.go
	rm -rf $(LOGS_DIR)
