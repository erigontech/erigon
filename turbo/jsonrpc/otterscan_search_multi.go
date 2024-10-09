package jsonrpc

func newCallFromToBlockProvider(isBackwards bool, callFromProvider, callToProvider BlockProvider) BlockProvider {
	var nextFrom, nextTo uint64
	var hasMoreFrom, hasMoreTo bool
	initialized := false

	return func() (uint64, bool, error) {
		if !initialized {
			initialized = true

			var err error
			if nextFrom, hasMoreFrom, err = callFromProvider(); err != nil {
				return 0, false, err
			}
			hasMoreFrom = hasMoreFrom || nextFrom != 0

			if nextTo, hasMoreTo, err = callToProvider(); err != nil {
				return 0, false, err
			}
			hasMoreTo = hasMoreTo || nextTo != 0
		}

		if !hasMoreFrom && !hasMoreTo {
			return 0, false, nil
		}

		var blockNum uint64
		if !hasMoreFrom {
			blockNum = nextTo
		} else if !hasMoreTo {
			blockNum = nextFrom
		} else {
			blockNum = nextFrom
			if isBackwards {
				if nextTo < nextFrom {
					blockNum = nextTo
				}
			} else {
				if nextTo > nextFrom {
					blockNum = nextTo
				}
			}
		}

		// Pull next; it may be that from AND to contains the same blockNum
		if hasMoreFrom && blockNum == nextFrom {
			var err error
			if nextFrom, hasMoreFrom, err = callFromProvider(); err != nil {
				return 0, false, err
			}
			hasMoreFrom = hasMoreFrom || nextFrom != 0
		}
		if hasMoreTo && blockNum == nextTo {
			var err error
			if nextTo, hasMoreTo, err = callToProvider(); err != nil {
				return 0, false, err
			}
			hasMoreTo = hasMoreTo || nextTo != 0
		}
		return blockNum, hasMoreFrom || hasMoreTo, nil
	}
}
