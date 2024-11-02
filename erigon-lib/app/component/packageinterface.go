package component

import (
	"context"
)

func CheckDependencyActivations(c *component) bool {
	return c.checkDependencyActivations()
}

func SetState(c *component, state State) {
	c.setState(state)
}

func SetContext(c *component, context context.Context) {
	c.setContext(context)
}

func OnDependenciesActive(context context.Context, c *component) error {
	return c.onDependenciesActive(context)
}
