fn config() -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");
    config.bytes(&["."]);
    config
}

fn make_protos(protos: &[&str]) {
    tonic_build::configure()
        .compile_with_config(config(), &protos, &["."])
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

    if cfg!(feature = "remotekv") {
        protos.push("remote/ethbackend.proto");
        protos.push("remote/kv.proto");
    }

    if cfg!(feature = "snapshotsync") {
        protos.push("snapshot_downloader/external_downloader.proto");
    }

    if cfg!(feature = "txpool") {
        protos.push("txpool/txpool.proto");
        protos.push("txpool/txpool_control.proto");
    }

    make_protos(&protos);
}
