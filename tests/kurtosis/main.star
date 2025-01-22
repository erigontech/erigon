EL_IMAGE = "erigon:latest"

EL_RPC_PORT_ID = "rpc"
EL_P2P_ETH67_PORT_ID = "p2p-eth67"
EL_P2P_ETH68_PORT_ID = "p2p-eth68"

EL_RPC_PORT = 8545
EL_P2P_ETH67_PORT = 30304
EL_P2P_ETH68_PORT = 30303

def run(plan):
    ctx_n2 = launch_el(plan, "el-n2", public_ports = {
        EL_RPC_PORT_ID: 8546,
    })

    ctx_n1 = launch_el(plan, "el-n1", enodes = [ctx_n2], public_ports = {
        EL_RPC_PORT_ID: 8545,
        EL_P2P_ETH68_PORT_ID: 30303,
    })

    ctx_n3 = launch_el(plan, "el-n3", enodes = [ctx_n1], public_ports = {
        EL_RPC_PORT_ID: 8547,
    })

def launch_el(plan, service_name, enodes = [], public_ports = {}):
    cmd = [
        "--chain=dev",
        "--datadir=/home/erigon/dev",
        "--http.addr=0.0.0.0",
        "--http.corsdomain=*",
        "--http.api=eth,erigon,web3,net,debug,trace,txpool,parity,admin",
        "--txpool.accountslots=30000",
        "--txpool.globalslots=30000"
    ]

    if len(enodes) > 0:
        cmd.append(
            "--staticpeers="
            + ",".join([build_enode_url(ctx.enode, host = ctx.ip_address) for ctx in enodes])
        )

    cfg = ServiceConfig(
        image = EL_IMAGE,
        cmd = cmd,
        ports = {
            EL_RPC_PORT_ID: PortSpec(
                number = EL_RPC_PORT,
                transport_protocol = "TCP"
            ),
            EL_P2P_ETH68_PORT_ID: PortSpec(
                number = EL_P2P_ETH68_PORT,
                transport_protocol = "TCP"
            ),
            EL_P2P_ETH67_PORT_ID: PortSpec(
                number = EL_P2P_ETH67_PORT,
                transport_protocol = "TCP"
            ),
        },
        public_ports = {
            port_id: PortSpec(number = public_ports[port_id], transport_protocol = "TCP")
            for port_id in public_ports
        }
    )

    service = plan.add_service(service_name, cfg)
    
    return struct (
        service_name = service_name,
        ip_address = service.ip_address,
        enode = get_enode_for_node(plan, service_name, service.ip_address),        
    )

def get_enode_for_node(plan, service_name, ip_addr):
    recipe = PostHttpRequestRecipe(
        endpoint="",
        body='{"method":"admin_nodeInfo","params":[],"id":1,"jsonrpc":"2.0"}',
        content_type="application/json",
        port_id=EL_RPC_PORT_ID,
        extract={
            "signature": """.result.enode  | split("@") | .[0] | split("//") | .[1]""",
            "host": """.result.enode  | split("@") | .[1] | split(":") | .[0]""",
            "port": """.result.enode  | split(":") | .[2] | split("?") | .[0]""",
        },
    )
    response = plan.wait(
        recipe=recipe,
        field="extract.signature",
        assertion="!=",
        target_value="",
        timeout="15m",
        service_name=service_name,
    )

    return struct (
        protocol = "enode",
        enode = response["extract.signature"],  
        host = response["extract.host"],
        port = response["extract.port"],
    )

def build_enode_url(enode, host=None, port=None):
    if host == None:
        host = enode.host
    if port == None:
        port = enode.port

    return "{}://{}@{}:{}".format(enode.protocol, enode.enode, host, port)
