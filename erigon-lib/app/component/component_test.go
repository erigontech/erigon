package component_test

import (
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/app/component"
	"github.com/stretchr/testify/require"
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
}
