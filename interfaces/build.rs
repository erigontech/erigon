fn config() -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.bytes(&["."]);
    config
}

fn make_protos(protos: &[&str]) {
    tonic_build::configure()
        .compile_with_config(config(), protos, &["."])
        .unwrap();
}

fn main() {
    let mut protos = vec!["types/types.proto"];

    if cfg!(feature = "consensus") {
        protos.push("consensus_engine/consensus.proto");
    }

    if cfg!(feature = "sentry") {
        protos.push("p2psentry/sentry.proto");
    }

    if cfg!(feature = "starknet") {
        protos.push("starknet/cairo.proto");
    }

    if cfg!(feature = "remotekv") {
        protos.push("remote/ethbackend.proto");
        protos.push("remote/kv.proto");
    }

    if cfg!(feature = "snapshotsync") {
        protos.push("downloader/downloader.proto");
    }

    if cfg!(feature = "txpool") {
        protos.push("txpool/mining.proto");
        protos.push("txpool/txpool.proto");
        protos.push("txpool/txpool_control.proto");
    }

    if cfg!(feature = "web3") {
        protos.push("web3/common.proto");
        protos.push("web3/eth.proto");
        protos.push("web3/trace.proto");
    }

    make_protos(&protos);
}
