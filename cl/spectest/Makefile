.PHONY: clean setup example run clean


tests:
	git clone https://github.com/ethereum/consensus-spec-tests
	cd consensus-spec-tests && git checkout 70dc28b18c71f3ae080c02f51bd3421e0b60609b && git lfs pull && cd ..
	mv consensus-spec-tests/tests .
	rm -rf consensus-spec-tests
	rm -rf tests/minimal
	# not needed for now
	rm -rf tests/mainnet/eip6110
	# will not implement until i see it on a testnet
	rm -rf tests/mainnet/deneb

clean:
	rm -rf junit.xml
	rm -rf test_report.html

mainnet:
	go test -tags=spectest -run=/mainnet -failfast -v

run:
	go test -v
