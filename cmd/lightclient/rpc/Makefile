all:
	protoc --proto_path=proto --go_out=lightrpc --go_opt=paths=source_relative proto/*.proto --go-grpc_out=lightrpc
	
	protoc-go-inject-tag -input="lightrpc/*.pb.go"
	sed -i 's/sszsize/ssz-size/g' lightrpc/*.pb.go
	sed -i 's/sszmax/ssz-max/g' lightrpc/*.pb.go
	go run github.com/ferranbt/fastssz/sszgen -path lightrpc/*.go -exclude-objs {{range $key, $val := .Aliases}}{{$key}},{{end}}Ignore

clean:
	rm lightrpc/*.pb.go