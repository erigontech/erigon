package spectest

import (
	"io/fs"
	"path/filepath"
	"testing"

	"gfx.cafe/util/go/generic"
	"github.com/stretchr/testify/require"
)

func RunCases(t *testing.T, app Appendix, root fs.FS) {
	cases, err := ReadTestCases(root)
	require.Nil(t, err, "reading cases")
	// prepare for gore.....
	type (
		K1 = string
		K2 = string
		K3 = string
		K4 = string
		K5 = string
		V  = TestCase
	)
	// welcome to hell
	cases.tree.Range0(func(s string, m *generic.Map5[K1, K2, K3, K4, K5, V]) bool {
		t.Run(s, func(t *testing.T) {
			t.Parallel()
			m.Range0(func(s string, m *generic.Map4[K1, K2, K3, K4, V]) bool {
				t.Run(s, func(t *testing.T) {
					t.Parallel()
					m.Range0(func(s string, m *generic.Map3[K1, K2, K3, V]) bool {
						t.Run(s, func(t *testing.T) {
							t.Parallel()
							m.Range0(func(s string, m *generic.Map2[K1, K2, V]) bool {
								t.Run(s, func(t *testing.T) {
									t.Parallel()
									m.Range0(func(s string, m *generic.Map1[K1, V]) bool {
										t.Run(s, func(t *testing.T) {
											t.Parallel()
											m.Range0(func(key string, value TestCase) bool {
												t.Run(key, func(t *testing.T) {
													require.NotPanics(t, func() {
														t.Parallel()
														runner, ok := app[value.RunnerName]
														if !ok {
															t.Skipf("runner not found: %s", value.RunnerName)
															return
														}
														handler, err := runner.GetHandler(value.HandlerName)
														if err != nil {
															t.Skipf("handler not found: %s", value.RunnerName)
															return
														}
														subfs, err := fs.Sub(root, filepath.Join(
															value.ConfigName,
															value.ForkPhaseName,
															value.RunnerName,
															value.HandlerName,
															value.SuiteName,
															value.CaseName,
														))
														require.NoError(t, err)
														err = handler.Run(t, subfs, value)
														require.NoError(t, err)
													})
												})
												return true
											})
										})
										return true
									})
								})
								return true
							})
						})
						return true
					})
				})
				return true
			})
		})
		return true
	})

}
