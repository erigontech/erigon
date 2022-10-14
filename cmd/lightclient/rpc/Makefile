all:
	protoc --proto_path=proto --go_out=lightrpc --go_opt=paths=source_relative proto/*.proto --go-grpc_out=lightrpc
		
clean:
	rm lightrpc/*.pb.go