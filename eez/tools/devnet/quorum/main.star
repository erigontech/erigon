# The eez-rollup0 devnet's L1 and deployment, its manager requiring two ECDSA
# proof systems, each attested by a reference eez-proof-signer, and a
# reference follower. No composer runs in the enclave: the chain's first
# composer runs on the host.

ethereum_package = import_module(
    "github.com/ethpandaops/ethereum-package/main.star@199620b24ac979c676010c5a68b2893c2bce4f1f"
)

SIGNER_PORT = 50061
FOLLOWER_RPC_PORT = 19688
FOLLOWER_ENGINE_PORT = 19684
FOLLOWER_P2P_PORT = 31640

# Hardhat development account 3, for private networks only.
SECOND_SIGNER_KEY = "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6"


def run(plan, args):
    eez = args.get("eez", {})
    eth_args = args["ethereum_package"]
    eth = ethereum_package.run(plan, eth_args)
    l1 = eth.all_participants[0].el_context
    chain_id = str(eth_args.get("network_params", {}).get("network_id", "7331"))
    deploy_image = eez.get("deploy_image", "eez-deploy:ci")
    poster_key = eez["poster_key"]
    keys = [eez["proof_signer_key"], eez.get("second_proof_signer_key", SECOND_SIGNER_KEY)]

    deploy = plan.run_sh(
        description="deploy EEZ, one ECDSA proof system and the rollup manager; generate the L2 genesis",
        image=deploy_image,
        env_vars={
            "EEZ_L1_RPC_URL": l1.rpc_http_url,
            "EEZ_L1_POSTER_KEY": poster_key,
            "EEZ_PROOF_SIGNER_KEY": keys[0],
            "EEZ_L2_RECOVERY_ADDRESS": eez["recovery_address"],
            "EEZ_DEPLOYMENTS_FILE": "/out/deployments.env",
            "EEZ_GENESIS_OUT": "/out/l2-genesis.json",
        },
        run="mkdir -p /out && bash /repo/scripts/deploy.sh",
        store=[StoreSpec(src="/out", name="eez-deployments")],
        wait="900s",
    )

    script = plan.upload_files(src="add-proof-system.sh", name="eez-add-proof-system")
    quorum = plan.run_sh(
        description="add a second ECDSA proof system and set the threshold to 2",
        image=deploy_image,
        env_vars={
            "EEZ_L1_RPC_URL": l1.rpc_http_url,
            "EEZ_DEPLOY_KEY": poster_key,
            "SIGNER_KEY": keys[1],
        },
        files={"/out": deploy.files_artifacts[0], "/scripts": script},
        run="bash /scripts/add-proof-system.sh",
        store=[StoreSpec(src="/quorum", name="eez-quorum")],
        wait="600s",
    )

    for i, key in enumerate(keys):
        files = {"/out": deploy.files_artifacts[0]}
        overrides = ""
        if i > 0:
            files["/quorum"] = quorum.files_artifacts[0]
            overrides = ". /quorum/quorum.env;"
        plan.add_service(
            name="eez-proof-signer-{}".format(i + 1),
            config=ServiceConfig(
                image=eez.get("proof_signer_image", "eez-proof-signer:ci"),
                ports={
                    "grpc": PortSpec(number=SIGNER_PORT, transport_protocol="TCP", wait="2m"),
                },
                files=files,
                env_vars={
                    "EEZ_PROOF_SIGNER_KEY": key,
                    "RUST_LOG": eez.get("proof_signer_rust_log", "info"),
                },
                entrypoint=["/bin/sh", "-c"],
                cmd=[
                    " ".join(
                        [
                            "set -eu; set -a; . /out/deployments.env;",
                            overrides,
                            "set +a; exec eez-proof-signer",
                            "--listen-addr=0.0.0.0:{}".format(SIGNER_PORT),
                            "--chain-config=/out/l2-genesis.json",
                        ]
                    )
                ],
            ),
        )

    plan.add_service(
        name="eez-follower",
        config=ServiceConfig(
            image=eez.get("eez_node_image", "eez-node:ci"),
            ports={
                "l2-rpc": PortSpec(
                    number=FOLLOWER_RPC_PORT,
                    transport_protocol="TCP",
                    application_protocol="http",
                ),
            },
            files={"/out": deploy.files_artifacts[0]},
            env_vars={
                "EEZ_L1_RPC_URL": l1.rpc_http_url,
                "EEZ_L1_CHAIN_ID": chain_id,
                "EEZ_L1_BLOCK_TIME_MS": str(eez.get("l1_block_time_ms", 12000)),
                "EEZ_L2_BLOCK_TIME_MS": str(eez.get("l2_block_time_ms", 2000)),
                "EEZ_PROOF_TIME_MS": str(eez.get("proof_time_ms", 5000)),
                "EEZ_SUBMISSION_SLACK_MS": str(eez.get("submission_slack_ms", 500)),
                "EEZ_SYNC_L1_ONLY": "1",
                "EEZ_L2_RPC_URL": "http://127.0.0.1:{}".format(FOLLOWER_RPC_PORT),
                "EEZL2_ADDRESS": "0x4200000000000000000000000000000000000007",
                "RUST_LOG": "info",
            },
            entrypoint=["/bin/sh", "-c"],
            cmd=[
                " ".join(
                    [
                        "set -eu; set -a; . /out/deployments.env; set +a;",
                        "exec eez-follower node",
                        "--chain=/out/l2-genesis.json",
                        "--datadir=/data/l2",
                        "--log.stdout.format=json",
                        "--log.file.max-files=0",
                        "--http --http.addr=0.0.0.0 --http.port={} --http.api=eth,net,web3,debug".format(FOLLOWER_RPC_PORT),
                        "--authrpc.addr=127.0.0.1 --authrpc.port={}".format(FOLLOWER_ENGINE_PORT),
                        "--port={0} --discovery.port={0} --discovery.v5.port={1}".format(FOLLOWER_P2P_PORT, FOLLOWER_P2P_PORT + 1),
                        "--ipcdisable --disable-discovery",
                    ]
                )
            ],
        ),
    )
