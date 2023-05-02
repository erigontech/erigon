package consensus_tests

import "github.com/ledgerwatch/erigon/cmd/ef-tests-cl/consensus_tests/spectest"

var TestFormats = spectest.Appendix{}

func init() {
	TestFormats.Add("bls").
		With("aggregate_verify", &BlsAggregateVerify{}).
		With("aggregate", &spectest.UnimplementedHandler{}).
		With("eth_aggregate_pubkeys", &spectest.UnimplementedHandler{}).
		With("eth_fast_aggregate_verify", &spectest.UnimplementedHandler{}).
		With("fast_aggregate_verify", &spectest.UnimplementedHandler{}).
		With("sign", &spectest.UnimplementedHandler{}).
		With("verify", &spectest.UnimplementedHandler{})
	TestFormats.Add("epoch_processing").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("finality").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("fork-choice").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("forks").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("genesis").
		With("validity", &spectest.UnimplementedHandler{}).
		With("initialization", &spectest.UnimplementedHandler{})
	TestFormats.Add("kzg").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("light_client").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("operations").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("random").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("rewards").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("sanity").
		With("slots", &spectest.UnimplementedHandler{}).
		With("blocks", &spectest.UnimplementedHandler{})
	TestFormats.Add("shuffling").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("ssz_generic").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("ssz_static").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("sync").
		With("", &spectest.UnimplementedHandler{})
	TestFormats.Add("transition").
		With("", &spectest.UnimplementedHandler{})
}
