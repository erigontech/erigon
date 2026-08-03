// A batch mixing a real call with a response-shaped message must still reply to the real call.

--> [{"jsonrpc":"2.0","id":1,"method":"test_echo","params":["x",1]},{"jsonrpc":"2.0","id":2,"result":"0x1"}]
<-- [{"jsonrpc":"2.0","id":1,"result":{"String":"x","Int":1,"Args":null}}]

// A batch mixing a real call with a subscription notification must still reply to the real call.

--> [{"jsonrpc":"2.0","id":3,"method":"test_echo","params":["x",2]},{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x1","result":"0x1"}}]
<-- [{"jsonrpc":"2.0","id":3,"result":{"String":"x","Int":2,"Args":null}}]
