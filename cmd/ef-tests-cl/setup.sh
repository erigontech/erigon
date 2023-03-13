git clone https://github.com/ethereum/consensus-spec-tests
cd consensus-spec-tests && git lfs pull && cd ..
mv consensus-spec-tests/tests .
rm -rf consensus-spec-tests
rm -rf tests/minimal #these ones are useless