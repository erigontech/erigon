# Debug rpc cache

A handy tool for caching responses from the L1 and for chaos testing events that
rely on L1 interactions

## Normal cache

To run the cache in the standard way you can just use `go run main.go` and 
the service will run on port 6969.

To use the cache ensure that your node is calling in the following format
`http://localhost:6969?chainid=<chainid>&endpoint=<endpoint>`

Where endpoint is the upstream endpoint that you want to cache responses from.

The tool can be used across multiple networks which are differentiated by the `chainid`
in the URL.

The application stores everything in a bolt db that will be created local to wherever
the code is run from

## Chaos mode

To add a little chaos you can use the flags below which will alter resposnes sent 
from the tool.  There is configurable randomness and you can specifiy specific addresses
or individual topics or combinations of the two

`-chaos=5` - this will apply chaos 1 in 5 times
`-chaos-after=1000000` - this will apply chaos after the specified block number for eth_getLogs by inspecting the `fromBlock` argument
`-chaos-topics=0x1234567890abcdef1234567890abcdef12345678,0x1234567890abcdef1234567890abcdef12345678` - this will remove any log events that contain these topics
`-chaos-addresses=0x1234567890abcdef1234567890abcdef12345678,0x1234567890abcdef1234567890abcdef12345678` - this will remove any log events that contain these addresses