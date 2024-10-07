DOC_NAME:="endpoints.md"

.PHONY: gen-doc
gen-doc: 
	go run main.go $(DOC_NAME)

.PHONY: check-doc
check-doc: 
	go run main.go tmp$(DOC_NAME)
	cmp -s ./$(DOC_NAME) ./tmp$(DOC_NAME); \
	RETVAL=$$?; \
	rm ./tmp$(DOC_NAME); \
	exit $$RETVAL