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

example: tests
	go test -tags=spectest -v -run=/mainnet/altair/sanity

run: tests
	go test -v
