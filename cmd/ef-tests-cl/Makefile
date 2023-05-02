.PHONY: build clean setup example


setup:
	git clone https://github.com/ethereum/consensus-spec-tests
	cd consensus-spec-tests && git lfs pull && cd ..
	mv consensus-spec-tests/tests .
	rm -rf consensus-spec-tests
	rm -rf tests/minimal

clean:
	rm -rf tests

build:
	go build -o eftests ./

example:
	#go test -v -run=/mainnet/altair/transition
	go test -v -run=/mainnet/altair/sanity
