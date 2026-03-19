package p2p

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"net"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/p2p/discover"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func newLocalNode(
	ctx context.Context,
	privKey *ecdsa.PrivateKey,
	ipAddr net.IP,
	udpPort, tcpPort int,
	tmpDir string,
	logger log.Logger,
) (*enode.LocalNode, error) {
	db, err := enode.OpenDBEx(ctx, "", tmpDir, logger)
	if err != nil {
		return nil, fmt.Errorf("could not open node's peer database: %w", err)
	}
	localNode := enode.NewLocalNode(db, privKey)

	udpEntry := enr.UDP(udpPort)
	tcpEntry := enr.TCP(tcpPort)

	localNode.Set(udpEntry)
	localNode.Set(tcpEntry)
	localNode.SetFallbackUDP(udpPort)

	if ipAddr.IsUnspecified() {
		if detected := detectOutboundIP(ipAddr); detected != nil {
			logger.Info("[Caplin] Discovery address is unspecified, using detected outbound IP for ENR. Set --caplin.discovery.addr explicitly to override", "detected", detected)
			ipAddr = detected
		} else {
			logger.Warn("[Caplin] Discovery address is unspecified and outbound IP detection failed, ENR will have no IP. Set --caplin.discovery.addr to your public IP")
		}
	}
	if !ipAddr.IsUnspecified() {
		localNode.Set(enr.IP(ipAddr))
		localNode.SetFallbackIP(ipAddr)
	}

	return localNode, nil
}

// detectOutboundIP determines the preferred outbound IP address by asking the
// OS routing table (no actual traffic is sent). Returns nil if detection fails.
func detectOutboundIP(unspecified net.IP) net.IP {
	network, target := "udp4", "8.8.8.8:80"
	if unspecified.To4() == nil {
		network, target = "udp6", "[2001:4860:4860::8888]:80"
	}
	conn, err := net.Dial(network, target)
	if err != nil {
		return nil
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP
}

func NewUDPv5Listener(ctx context.Context, cfg *P2PConfig, discCfg discover.Config, logger log.Logger) (*discover.UDPv5, error) {
	var (
		ipAddr = cfg.IpAddr
		port   = cfg.Port
	)

	ip := net.ParseIP(ipAddr)
	if ip == nil {
		return nil, fmt.Errorf("bad ip address provided, %s was provided", ipAddr)
	}

	var bindIP net.IP
	var networkVersion string
	// If the IP is an IPv4 address, bind to the correct zero address.
	if ip.To4() != nil {
		bindIP, networkVersion = ip.To4(), "udp4"
	} else {
		bindIP, networkVersion = ip.To16(), "udp6"
	}

	udpAddr := &net.UDPAddr{
		IP:   bindIP,
		Port: port,
	}
	conn, err := net.ListenUDP(networkVersion, udpAddr)
	if err != nil {
		return nil, err
	}

	localNode, err := newLocalNode(ctx, discCfg.PrivateKey, ip, port, int(cfg.TCPPort), cfg.TmpDir, logger)
	if err != nil {
		return nil, err
	}

	// Start stream handlers
	net, err := discover.ListenV5(conn, localNode, discCfg)
	if err != nil {
		return nil, err
	}
	return net, nil
}
