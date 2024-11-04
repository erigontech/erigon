package app

import (
	"bytes"
	"context"
	"encoding"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/erigontech/erigon-lib/common/hton"
	"github.com/erigontech/erigon-lib/common/ntoh"
)

var resolvedDomains = map[DomainId]Domain{}
var resolvedDomainsLock = sync.RWMutex{}

// an ident is an immutable string of bytes
type ident string

func (id ident) asBytes() []byte {
	return unsafe.Slice(unsafe.StringData(string(id)), len(id))
}

func asIdent(bytes []byte) ident {
	return ident(unsafe.String(unsafe.SliceData(bytes), len(bytes)))
}

type Domain interface {
	encoding.TextMarshaler
	encoding.TextUnmarshaler

	Id() DomainId
	RootId() DomainId
	Name() string

	CompareTo(other interface{}) int
	Matches(other Domain) bool
	Equals(other Domain) bool
	String() string

	NewId(context context.Context, entity ...any) (Id, error)

	Description() string
	Source() string
}

type DomainFactory interface {
	New(context context.Context) (Domain, error)
}

type DomainFactoryFunc func(context context.Context) (Domain, error)

func (f DomainFactoryFunc) New(context context.Context) (Domain, error) {
	return f(context)
}

func CompareDomains(a, b interface{}) int {
	ad := a.(Domain)
	bd := b.(Domain)
	return bytes.Compare(ad.Id().value.asBytes(), bd.Id().value.asBytes())
}

type domain[T comparable] struct {
	id          DomainId
	root        Domain
	incarnation Incarnation
	idGenerator IdGenerator[T]
	info        map[any]any
}

type domainFeature struct {
	target     reflect.Type
	applicator func(t any)
}

func (o domainFeature) apply(t any) {
	o.applicator(t)
}

func withFeature[T any](applicator func(t *T)) domainFeature {
	var t T
	return domainFeature{
		target:     reflect.TypeOf(t),
		applicator: func(t interface{}) { applicator(t.(*T)) },
	}
}

func WithIdGenerator[T comparable](g IdGenerator[T]) domainFeature {
	return withFeature[domain[T]](func(d *domain[T]) { d.idGenerator = g })
}

func WithInfo[T comparable](info map[any]any) domainFeature {
	return withFeature[domain[T]](func(d *domain[T]) {
		if len(info) > 0 {
			if d.info == nil {
				d.info = make(map[interface{}]interface{})
			}
			for key, value := range info {
				info[key] = value
			}
		}
	})
}

type DomainId Handle[ident]

func (id DomainId) Get(context context.Context) (Domain, error) {
	resolvedDomainsLock.RLock()
	resolved, ok := resolvedDomains[id]
	resolvedDomainsLock.RUnlock()

	if ok {
		return resolved, nil
	}

	return nil, errors.New("domain not found")
}

func (id DomainId) String() string {
	v := Handle[ident](id).Value()

	if v[0] < 9 {
		return strconv.Itoa(int(ntoh.UInt([]byte(v), 1, int(v[0]))))
	}

	return string(v)
}

func (id DomainId) Domain() Domain {
	if domain, err := id.Get(context.Background()); err == nil {
		return domain
	}

	return nil
}

func (id DomainId) TypeId() TypeId {
	if typedDomain, ok := id.Domain().(TypedDomain); ok {
		return typedDomain.TypeId()
	}

	return nil
}

func (id DomainId) asBytes() []byte {
	return Handle[ident](id).Value().asBytes()
}

func toId(rootId Handle[ident], incarnation Incarnation, _ ...domainFeature) (DomainId, error) {
	// TODO this needs rlp encoding
	if incarnation != nil {
		return DomainId(Make(ident(strings.Join([]string{string(rootId.Value()), string(incarnation.asIdent())}, "-")))), nil
	}

	return DomainId(rootId), nil
}

func NewDomain[T comparable](features ...domainFeature) (Domain, error) {
	nextDomainLock.Lock()
	defer nextDomainLock.Unlock()

	// keep the underlying numeric value so that ids sort
	// in numeric rather than alphabetic order
	len := hton.UIntLen(nextDomainId)
	idbuf := make([]byte, len+1)
	hton.UInt(idbuf[1:], 0, nextDomainId)
	idbuf[0] = byte(len)
	d, err := newDomain[T](asIdent(idbuf), nil, features...)

	if err != nil {
		return nil, err
	}

	nextDomainId++

	return d, nil
}

var nextDomainId uint64 = 0
var nextDomainLock = sync.Mutex{}

func NewNamedDomain[T comparable](name string, features ...domainFeature) (Domain, error) {
	return newDomain[T](ident(strings.ToLower(name)), nil, features...)
}

func newDomain[T comparable](rootId ident, incarnation Incarnation, features ...domainFeature) (Domain, error) {
	id, err := toId(Make(rootId), incarnation, features...)

	if err != nil {
		return nil, err
	}

	if domain, _ := id.Get(context.Background()); domain != nil {
		return domain, nil
	}

	var info map[interface{}]interface{}

	d := &domain[T]{
		id:          id,
		incarnation: incarnation,
		info:        info}

	for _, feature := range features {
		feature.apply(d)
	}

	resolvedDomainsLock.Lock()
	defer resolvedDomainsLock.Unlock()
	resolvedDomains[d.Id()] = d

	return d, nil
}

func (d *domain[T]) Equals(domain Domain) bool {
	return d.CompareTo(domain) == 0
}

func (d *domain[T]) CompareTo(other interface{}) int {
	if other == nil {
		return 1
	}

	switch otherTyped := other.(type) {
	case Domain:
		return bytes.Compare(d.id.asBytes(), otherTyped.Id().asBytes())
	}

	return -1
}

func (d *domain[T]) String() string {
	// TODO this likely need some formatting
	return d.id.String()
}

func (d *domain[T]) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *domain[T]) UnmarshalText(text []byte) error {
	return errors.New("TODO")
}

func (d *domain[T]) Matches(other Domain) bool {
	// TODO
	return d.Equals(other)
}

func (d *domain[T]) Id() DomainId {
	return d.id
}

func (d *domain[T]) RootId() DomainId {
	if d.root != nil {
		return d.root.Id()
	}

	return DomainId{}
}

func (d *domain[T]) Incarnation() Incarnation {
	return d.incarnation
}

func (d *domain[T]) IsRootOf(domain Domain) bool {
	return d.IsRoot() && d.Matches(domain)
}

func (d *domain[T]) Root() Domain {
	if d.IsRoot() {
		return d
	}

	if d.root != nil {
		return d.root
	}

	return nil
}

func (d *domain[T]) IsRoot() bool {
	return d.incarnation == nil
}

func (d *domain[T]) ToId(id T) (Id, error) {
	return NewId(d, id)
}

func (d *domain[T]) Name() string {
	name, ok := d.info["name"]

	if ok {
		return name.(string)
	}

	if d.root != nil {
		return d.root.Name()
	}

	return ""
}

func (d *domain[T]) Description() string {
	description, ok := d.info["description"]

	if ok {
		return description.(string)
	}

	if d.root != nil {
		return d.root.Description()
	}

	return ""
}

func (d *domain[T]) Source() string {
	source, ok := d.info["source"]

	if ok {
		return source.(string)
	}

	if d.root != nil {
		return d.root.Source()
	}

	return ""
}

func (d *domain[T]) InfoValue(key interface{}) interface{} {
	if d.info != nil {
		value, ok := d.info[key]
		if ok {
			return value
		}
	}

	return nil
}

func (d *domain[T]) IdGenerator() IdGenerator[T] {
	return d.idGenerator
}

func (d *domain[T]) NewId(generationContext context.Context, entity ...interface{}) (Id, error) {
	if d.idGenerator != nil {
		var e interface{}

		if len(entity) > 0 {
			e = entity[0]
		}

		idval, err := d.idGenerator.GenerateId(generationContext, e)

		if err != nil {
			return nil, err
		}

		id, err := d.ToId(idval)

		if err != nil {
			return nil, err
		}

		return id, nil
	}

	return nil, errors.New("id creation not initialized")
}

func NewTypedDomain[I comparable, T any](features ...domainFeature) (TypedDomain, error) {
	return newTypedDomain[I, T](features...)
}

func newTypedDomain[I comparable, T any](_ ...domainFeature) (TypedDomain, error) {

	localDomain, _ := NewDomain[I]()

	var t T
	return &typedDomain[I, T]{
		localDomain.(*domain[I]),
		TypeIdOf(t),
	}, nil
}

type TypedDomain interface {
	Domain
	Includes(typeId TypeId) bool
	TypeId() TypeId
}

type typedDomain[I comparable, T any] struct {
	*domain[I]
	typeId TypeId
}

func (typedDomain *typedDomain[I, T]) Includes(typeId TypeId) bool {
	isFor := typedDomain.typeId.AssignableTo(typeId)
	return isFor
}

func (typedDomain *typedDomain[I, T]) TypeId() TypeId {
	if typedDomain.typeId != nil {
		return typedDomain.typeId
	}

	if typedRoot, ok := typedDomain.root.(TypedDomain); ok && typedRoot.TypeId() != nil {
		return typedRoot.TypeId()
	}

	return nil
}
