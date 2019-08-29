package tests

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"math/big"
	"testing"
)

func TestStateDebug(t *testing.T) {
	st := new(testMatcher)
	st.whitelist(`^TestStateDebug/stSStoreTest/InitCollision.json`)
	st.walk(t, stateTestDir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			name = name + "/" + key
			i := 0
			t.Run(key, func(t *testing.T) {
				fmt.Println("tests/state_test.go:43 ", name)
				withTrace(t, test.gasLimit(subtest), func(vmconfig vm.Config) error {
					config, ok := Forks[subtest.Fork]
					if !ok {
						return UnsupportedForkError{subtest.Fork}
					}
					ctx := config.WithEIPsFlags(context.Background(), big.NewInt(1))
					fmt.Println("--------------- i", i, key)
					_, _, _, err := test.Run(ctx, subtest, vmconfig)
					i++
					return st.checkFailure(t, err)
				})
			})
		}
	})

}

//st.fails(`^stCreate2/RevertInCreateInInitCreate2.json/Constantinople/0`, "work in progress")
//st.fails(`^stCreate2/RevertInCreateInInitCreate2.json/ConstantinopleFix/0`, "work in progress")

//st.fails(`^stExtCodeHash/dynamicAccountOverwriteEmpty.json/Constantinople/0`, "work in progress")
//st.fails(`^stExtCodeHash/dynamicAccountOverwriteEmpty.json/ConstantinopleFix/0`, "work in progress")
/*
st.fails(`^stRevertTest/RevertInCreateInInit.json/Byzantium/0`, "work in progress")
st.fails(`^stRevertTest/RevertInCreateInInit.json/Constantinople/0`, "work in progress")
st.fails(`^stRevertTest/RevertInCreateInInit.json/ConstantinopleFix/0`, "work in progress")

*/

func TestStateDebug2(t *testing.T) {
	st := new(testMatcher)
	st.whitelist(`^TestStateDebug2/stCreate2/create2collisionStorage.json`)
	st.walk(t, stateTestDir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			name = name + "/" + key
			i := 0
			t.Run(key, func(t *testing.T) {
				fmt.Println("tests/state_test.go:43 ", name)
				withTrace(t, test.gasLimit(subtest), func(vmconfig vm.Config) error {
					config, ok := Forks[subtest.Fork]
					if !ok {
						return UnsupportedForkError{subtest.Fork}
					}
					ctx := config.WithEIPsFlags(context.Background(), big.NewInt(1))
					fmt.Println("--------------- i", i, key)
					_, _, _, err := test.Run(ctx, subtest, vmconfig)
					i++
					return st.checkFailure(t, err)
				})
			})
		}
	})

}

func TestStateDebug3(t *testing.T) {
	st := new(testMatcher)
	st.whitelist(`^TestStateDebug3/stRevertTest/RevertInCreateInInit.json`)
	st.walk(t, stateTestDir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			name = name + "/" + key
			i := 0
			t.Run(key, func(t *testing.T) {
				fmt.Println("tests/state_test.go:43 ", name)
				withTrace(t, test.gasLimit(subtest), func(vmconfig vm.Config) error {
					config, ok := Forks[subtest.Fork]
					if !ok {
						return UnsupportedForkError{subtest.Fork}
					}
					ctx := config.WithEIPsFlags(context.Background(), big.NewInt(1))
					fmt.Println("--------------- i", i, key)
					_, _, _, err := test.Run(ctx, subtest, vmconfig)
					i++
					return st.checkFailure(t, err)
				})
			})
		}
	})

}

func TestStateDebug4(t *testing.T) {
	st := new(testMatcher)
	st.whitelist(`^TestStateDebug4/stExtCodeHash/dynamicAccountOverwriteEmpty.json`)
	st.walk(t, stateTestDir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			name = name + "/" + key
			i := 0
			t.Run(key, func(t *testing.T) {
				fmt.Println("tests/state_test.go:43 ", name)
				withTrace(t, test.gasLimit(subtest), func(vmconfig vm.Config) error {
					config, ok := Forks[subtest.Fork]
					if !ok {
						return UnsupportedForkError{subtest.Fork}
					}
					ctx := config.WithEIPsFlags(context.Background(), big.NewInt(1))
					fmt.Println("--------------- i", i, key)
					_, _, _, err := test.Run(ctx, subtest, vmconfig)
					i++
					return st.checkFailure(t, err)
				})
			})
		}
	})

}
func TestStateDebug5(t *testing.T) {
	st := new(testMatcher)
	st.whitelist(`^TestStateDebug5/stCreate2/RevertInCreateInInitCreate2.json`)
	st.walk(t, stateTestDir, func(t *testing.T, name string, test *StateTest) {
		for _, subtest := range test.Subtests() {
			subtest := subtest
			key := fmt.Sprintf("%s/%d", subtest.Fork, subtest.Index)
			name = name + "/" + key
			i := 0
			t.Run(key, func(t *testing.T) {
				fmt.Println("tests/state_test.go:43 ", name)
				withTrace(t, test.gasLimit(subtest), func(vmconfig vm.Config) error {
					config, ok := Forks[subtest.Fork]
					if !ok {
						return UnsupportedForkError{subtest.Fork}
					}
					ctx := config.WithEIPsFlags(context.Background(), big.NewInt(1))
					fmt.Println("--------------- i", i, key)
					_, _, _, err := test.Run(ctx, subtest, vmconfig)
					i++
					return st.checkFailure(t, err)
				})
			})
		}
	})

}
