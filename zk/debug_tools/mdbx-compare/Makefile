ifeq ($(words $(ARGS)),2)
  # ARGS is OK
else
  $(error "Usage: make infotree ARGS=\"datadir1 datadir2\"")
endif

## infotree
.PHONY: infotree
infotree:
	@echo "running mdbx-compare for infotree"
	@chmod +x infotree-compare.sh
	@./infotree-compare.sh $(ARGS)

## make infotree ARGS="/path/to/data1/chaindata /path/to/data2/chaindata"