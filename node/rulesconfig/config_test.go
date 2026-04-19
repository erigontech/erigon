package rulesconfig

import (
	"testing"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

func TestActiveRulesName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *chain.Config
		want chain.RulesName
	}{
		{
			name: "defaults to ethash",
			cfg:  &chain.Config{},
			want: chain.EtHashRules,
		},
		{
			name: "aura takes priority",
			cfg: &chain.Config{
				Aura: &chain.AuRaConfig{},
				Bor: &borcfg.BorConfig{
					ValidatorContract: "0x0000000000000000000000000000000000001000",
				},
			},
			want: chain.AuRaRules,
		},
		{
			name: "bor requires validator contract",
			cfg: &chain.Config{
				Bor: &borcfg.BorConfig{
					ValidatorContract: "0x0000000000000000000000000000000000001000",
				},
			},
			want: chain.BorRules,
		},
		{
			name: "bor without validator contract falls back to ethash",
			cfg: &chain.Config{
				Bor: &borcfg.BorConfig{},
			},
			want: chain.EtHashRules,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ActiveRulesName(tt.cfg); got != tt.want {
				t.Fatalf("ActiveRulesName() = %q, want %q", got, tt.want)
			}
		})
	}
}
