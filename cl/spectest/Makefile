.PHONY: clean setup example run clean


tests:
	git clone https://github.com/ethereum/consensus-spec-tests
	cd consensus-spec-tests && git lfs pull && cd ..
	mv consensus-spec-tests/tests .
	rm -rf consensus-spec-tests
	rm -rf tests/minimal

clean:
	rm -rf junit.xml
	rm -rf test_report.html

mainnet:
	go test -tags=spectest -run=/mainnet

run:
	go test -v
