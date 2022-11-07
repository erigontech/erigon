all:
	protoc --proto_path=proto --go_out=consensusrpc --go_opt=paths=source_relative proto/*.proto --go-grpc_out=consensusrpc
		
clean:
	rm lightrpc/*.pb.go