# Debug Flags

--debug.limit
- This flag sets an upper block-height limit which the node will run to e.g. block 100
- At this height the node will exit with exit code 0 success, or 1/2 for failure

--debug.step
- This flag sets a 'step size' of blocks which causes each stage to move on in the stage loop, for example if this is set to 10, then stage_batches will consume 10 blocks from the stream, execution and interhashes will also run for 10 blocks, before returning
- This allows us to 'check the stateroot' every n blocks

--debug.step-after
- Sets the minimum block height to reach before 'stepping' behaviour will start

Example:

--debug.limit=100 --debug.step=10 --debug.step-after=50

The node will sync to block height 50 in one run of the stage loop, then consume 10 blocks up to 60, 70... all the way to 100 before it then exits at this block height.

This is useful because we can pin down where the state starts to mismatch in the event of an execution failure further down the chain. With the exit codes, we can also use the bisector to help us leave this as a standalone job in order to find us the 'bad block' for further investigation.
