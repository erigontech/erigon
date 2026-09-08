package epbs

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedMarginStrategyRejectsInvalidInputs(t *testing.T) {
	for name, test := range map[string]struct {
		margin float64
		value  *big.Int
	}{
		"nil value":       {margin: 0.85},
		"zero value":      {margin: 0.85, value: big.NewInt(0)},
		"negative value":  {margin: 0.85, value: big.NewInt(-1)},
		"negative margin": {margin: -0.1, value: big.NewInt(100)},
		"margin above one": {
			margin: 1.1,
			value:  big.NewInt(100),
		},
		"not a number": {margin: math.NaN(), value: big.NewInt(100)},
		"infinity":     {margin: math.Inf(1), value: big.NewInt(100)},
	} {
		t.Run(name, func(t *testing.T) {
			strategy := FixedMarginStrategy{Margin: test.margin}
			require.Nil(t, strategy.Decide(1, test.value))
		})
	}
}

func TestFixedMarginStrategyEnforcesProfitFloor(t *testing.T) {
	strategy := FixedMarginStrategy{Margin: 0.85, MinProfit: big.NewInt(16)}
	require.Nil(t, strategy.Decide(1, big.NewInt(100)))

	strategy.MinProfit.SetInt64(15)
	require.Equal(t, big.NewInt(85), strategy.Decide(1, big.NewInt(100)))
}

func TestFixedMarginStrategyHandlesLargeValuesExactly(t *testing.T) {
	value := new(big.Int).Exp(big.NewInt(10), big.NewInt(19), nil)
	strategy := FixedMarginStrategy{Margin: 0.85}
	expected := new(big.Int).Mul(big.NewInt(85), new(big.Int).Exp(big.NewInt(10), big.NewInt(17), nil))
	require.Equal(t, expected, strategy.Decide(42, value))
}
