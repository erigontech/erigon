package clonable

type Clonable interface {
	Clone() Clonable
}
