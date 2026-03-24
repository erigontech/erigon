package util

type ExecPool interface {
	Exec(task func())
	PoolSize() int
	QueueSize() int
}
