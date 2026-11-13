package enr

// BT is the "bt" ENR key, which holds the BitTorrent listen port of the node.
// When present, peers can directly connect to the node's torrent client
// using the node's IP (from the "ip" key) and this port.
type BT uint16

func (v BT) ENRKey() string { return "bt" }
