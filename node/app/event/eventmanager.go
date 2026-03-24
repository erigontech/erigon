package event

type EventManager interface {
	Activate(bus *ServiceBus) error
	Deactivate() error

	IsActive() bool

	EventBus(key interface{}) *ManagedEventBus
}
