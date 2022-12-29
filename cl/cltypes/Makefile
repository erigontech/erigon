all:
	go run github.com/prysmaticlabs/fastssz/sszgen -path types.go 
	go run github.com/prysmaticlabs/fastssz/sszgen -path network.go 	
clean:
	rm lightrpc/*.pb.go