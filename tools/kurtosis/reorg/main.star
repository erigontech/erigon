PACKAGE = "github.com/ethpandaops/ethereum-package"
REVISION = "5795b77e65235ad223d4d3b413b2739190bf60cc"
ethereum = import_module(PACKAGE + "/main.star@" + REVISION)


def run(plan, args):
    configure_peers(plan, args)
    network = ethereum.run(plan, args)
    spamoor = args["spamoor_params"]
    config = plan.render_templates(
        {"spammers.yaml": struct(template=json.encode(spamoor["spammers"]), data={})},
        name="reorg-spammers",
    )
    # A shared client pool excludes the shorter branch during a partition.
    # Independent RPC endpoints and funding accounts keep every branch active.
    for index, participant in enumerate(network.all_participants):
        plan.add_service(
            name="spamoor-{}".format(index + 1),
            config=ServiceConfig(
                image=spamoor["image"],
                entrypoint=["./spamoor-daemon"],
                cmd=[
                    "--privkey={}".format(network.pre_funded_accounts[index].private_key),
                    "--rpchost={}".format(participant.el_context.rpc_http_url),
                    "--startup-spammer=/config/spammers.yaml",
                    "--slot-duration={}s".format(args["network_params"]["seconds_per_slot"]),
                ],
                ports={"http": PortSpec(number=8080, application_protocol="http")},
                files={"/config": config},
            ),
        )
    return network


def configure_peers(plan, args):
    # Fixed, trusted peers keep gossip penalties from extending a deliberate partition.
    # Reuse the package's public test identities; Disruptoor controls connectivity.
    participants = []
    peer_ids = []
    keys = PACKAGE + "/static_files/peerdas-node-keys/"
    for group in args["participants"]:
        for _ in range(group.get("count", 1)):
            participant = dict(group)
            participant["count"] = 1
            index = len(participants) + 1
            identity = json.decode(read_file(keys + "lodestar/node-key-file-{}/peer-id.json@{}".format(index, REVISION)))
            peer_ids.append(identity["id"])
            params = list(participant.get("cl_extra_params", []))
            if participant["cl_type"] == "lighthouse":
                source = "lighthouse/node-key-file-{}/key".format(index)
                mount = "/data/lighthouse/beacon-data/beacon/network"
            elif participant["cl_type"] == "teku":
                source = "teku/node-key-file-{}".format(index)
                mount = "/reorg-peer"
                params.extend([
                    "--Xp2p-private-key-file-secp256k1={}/node-key-file-{}".format(mount, index),
                    "--Xp2p-gossip-scoring-enabled=false",
                ])
            else:
                fail("Unsupported reorg-test CL: " + participant["cl_type"])
            key = plan.upload_files(src=keys + source + "@" + REVISION, name="reorg-peer-{}".format(index))
            participant["cl_extra_mounts"] = {mount: Directory(artifact_names=[key])}
            participant["cl_extra_params"] = params
            participants.append(participant)
    for index, participant in enumerate(participants):
        if participant["cl_type"] == "lighthouse":
            participant["cl_extra_params"].append("--trusted-peers=" + ",".join([
                peer_id for i, peer_id in enumerate(peer_ids) if i != index
            ]))
    args["participants"] = participants
