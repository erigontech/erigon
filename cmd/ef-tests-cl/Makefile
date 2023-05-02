.PHONY: clean setup example run


tests:
	git clone https://github.com/ethereum/consensus-spec-tests
	cd consensus-spec-tests && git lfs pull && cd ..
	mv consensus-spec-tests/tests .
	rm -rf consensus-spec-tests
	rm -rf tests/minimal

clean:
	rm -rf tests

example: tests
	go test -tags=spectest -v -run=/mainnet/altair/sanity

run: tests
	#go test -v -run=/mainnet/altair/transition
	go test -v -run=/mainnet/altair/sanity


