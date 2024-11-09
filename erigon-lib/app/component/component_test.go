package component_test

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/app/component"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

type provider struct {
}

func TestCreateComponent(t *testing.T) {
	c, err := component.NewComponent[provider](context.Background())
	require.Nil(t, err)
	require.NotNil(t, c)
	require.Equal(t, "root:provider", c.Id().String())

	var p *provider = c.Provider()
	require.NotNil(t, p)

	c1, err := component.NewComponent[provider](context.Background(),
		component.WithId("my-id"))
	require.Nil(t, err)
	require.NotNil(t, c1)
	require.Equal(t, "root:my-id", c1.Id().String())

	c2, err := component.NewComponent[provider](context.Background(),
		component.WithId("my-id-2"),
		component.WithDependencies(c, c1))
	require.Nil(t, err)
	require.NotNil(t, c2)
	require.Equal(t, "root:my-id-2", c2.Id().String())
	require.Equal(t, "root:my-id", c1.Id().String())
	require.Equal(t, "root:provider", c.Id().String())
	require.True(t, c.HasDependent(c2))
	require.True(t, c1.HasDependent(c2))
	require.False(t, c.HasDependent(c1))
	require.False(t, c1.HasDependent(c))
}

func TestCreateDomain(t *testing.T) {
	d, err := component.NewComponentDomain(context.Background(), "domain")
	require.Nil(t, err)
	require.NotNil(t, d)
	require.Equal(t, "root:domain", d.Id().String())
	d1, err := component.NewComponentDomain(context.Background(), "domain-1",
		component.WithDependentDomain(d))
	require.Nil(t, err)
	require.NotNil(t, d1)
	require.Equal(t, "domain:domain-1", d1.Id().String())
	d2, err := component.NewComponentDomain(context.Background(), "domain-2",
		component.WithDependentDomain(d1))
	require.Nil(t, err)
	require.NotNil(t, d2)
	require.Equal(t, "domain-1:domain-2", d2.Id().String())
}

func TestCreateComponentInDomain(t *testing.T) {
	d, err := component.NewComponentDomain(context.Background(), "domain")
	require.Nil(t, err)
	require.NotNil(t, d)
	require.Equal(t, "root:domain", d.Id().String())

	c, err := component.NewComponent[provider](context.Background(),
		component.WithDomain(d))
	require.Nil(t, err)
	require.NotNil(t, c)
	require.Equal(t, "domain:provider", c.Id().String())

	var p *provider = c.Provider()
	require.NotNil(t, p)

	c1, err := component.NewComponent[provider](context.Background())
	require.Nil(t, err)
	require.NotNil(t, c)
	require.Equal(t, "root:provider", c1.Id().String())

	d1, err := component.NewComponentDomain(context.Background(), "domain-1",
		component.WithDependencies(c1))
	require.Nil(t, err)
	require.NotNil(t, d1)
	require.Equal(t, "root:domain-1", d1.Id().String())
	require.Equal(t, "domain-1:provider", c1.Id().String())

	d2, err := component.NewComponentDomain(context.Background(), "domain-2",
		component.WithDependencies(c, c1))
	require.Nil(t, err)
	require.NotNil(t, d2)
	require.Equal(t, "root:domain-2", d2.Id().String())
	require.Equal(t, "domain-2:provider", c.Id().String())
	require.Equal(t, "domain-2:provider", c1.Id().String())
}

func mockProvider(ctrl *gomock.Controller, callCount int) *component.MockComponentProvider {
	p := component.NewMockComponentProvider(ctrl)
	p.EXPECT().
		Configure(gomock.Any(), gomock.Any()).
		Return(nil).
		Times(callCount)
	p.EXPECT().
		Initialize(gomock.Any(), gomock.Any()).
		Return(nil).
		Times(callCount)
	p.EXPECT().
		Recover(gomock.Any()).
		Return(nil).
		Times(callCount)
	p.EXPECT().
		Activate(gomock.Any()).
		Return(nil).
		Times(callCount)
	p.EXPECT().
		Deactivate(gomock.Any()).
		Return(nil).
		Times(callCount)
	return p
}
func TestComponentLifecycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	c, err := component.NewComponent[component.MockComponentProvider](context.Background(),
		component.WithProvider(mockProvider(ctrl, 1)))
	require.Nil(t, err)
	require.NotNil(t, c)
	require.Equal(t, "root:mockcomponentprovider", c.Id().String())

	err = c.Activate(context.Background())
	require.Nil(t, err)

	state, err := c.AwaitState(context.Background(), component.Active)
	require.Nil(t, err)
	require.Equal(t, component.Active, state)

	err = c.Deactivate(context.Background())
	require.Nil(t, err)

	state, err = c.AwaitState(context.Background(), component.Deactivated)
	require.Nil(t, err)
	require.Equal(t, component.Deactivated, state)

	d, err := component.NewComponent[component.MockComponentProvider](context.Background(),
		component.WithId("d"),
		component.WithProvider(mockProvider(ctrl, 1)))
	require.Nil(t, err)
	require.NotNil(t, d)
	require.Equal(t, "root:d", d.Id().String())

	c1, err := component.NewComponent[component.MockComponentProvider](context.Background(),
		component.WithId("c1"),
		component.WithProvider(mockProvider(ctrl, 1)),
		component.WithDependencies(d))
	require.Nil(t, err)
	require.NotNil(t, c1)
	require.Equal(t, "root:c1", c1.Id().String())

	err = c1.Activate(context.Background())
	require.Nil(t, err)

	state, err = c1.AwaitState(context.Background(), component.Active)
	require.Nil(t, err)
	require.Equal(t, component.Active, state)
	require.Equal(t, component.Active, d.State())
	require.Equal(t, component.Deactivated, c.State())

	err = c1.Deactivate(context.Background())
	require.Nil(t, err)

	state, err = c1.AwaitState(context.Background(), component.Deactivated)
	require.Nil(t, err)
	require.Equal(t, component.Deactivated, state)
	require.Equal(t, component.Deactivated, d.State())
	require.Equal(t, component.Deactivated, c.State())

	d1, err := component.NewComponent[component.MockComponentProvider](context.Background(),
		component.WithId("d1"),
		component.WithProvider(mockProvider(ctrl, 1)))
	require.Nil(t, err)
	require.NotNil(t, d1)
	require.Equal(t, "root:d1", d1.Id().String())

	d2, err := component.NewComponent[component.MockComponentProvider](context.Background(),
		component.WithId("d2"),
		component.WithProvider(mockProvider(ctrl, 1)))
	require.Nil(t, err)
	require.NotNil(t, d2)
	require.Equal(t, "root:d2", d2.Id().String())

	d3, err := component.NewComponent[component.MockComponentProvider](context.Background(),
		component.WithId("d3"),
		component.WithProvider(mockProvider(ctrl, 1)))
	require.Nil(t, err)
	require.NotNil(t, d3)
	require.Equal(t, "root:d3", d3.Id().String())

	c2, err := component.NewComponent[component.MockComponentProvider](context.Background(),
		component.WithId("c2"),
		component.WithProvider(mockProvider(ctrl, 1)),
		component.WithDependencies(d1, d2, d3))

	require.Nil(t, err)
	require.NotNil(t, c1)
	require.Equal(t, "root:c2", c2.Id().String())

	err = c2.Activate(context.Background())
	require.Nil(t, err)

	state, err = c2.AwaitState(context.Background(), component.Active)
	require.Nil(t, err)
	require.Equal(t, component.Active, state)
	require.Equal(t, component.Active, d1.State())
	require.Equal(t, component.Active, d2.State())
	require.Equal(t, component.Active, d3.State())
	require.Equal(t, component.Deactivated, d.State())
	require.Equal(t, component.Deactivated, c1.State())
	require.Equal(t, component.Deactivated, c.State())

	err = c2.Deactivate(context.Background())
	require.Nil(t, err)

	state, err = c2.AwaitState(context.Background(), component.Deactivated)
	require.Nil(t, err)
	require.Equal(t, component.Deactivated, state)
	require.Equal(t, component.Deactivated, d1.State())
	require.Equal(t, component.Deactivated, d2.State())
	require.Equal(t, component.Deactivated, d3.State())
	require.Equal(t, component.Deactivated, d.State())
	require.Equal(t, component.Deactivated, c1.State())
	require.Equal(t, component.Deactivated, c.State())
}
