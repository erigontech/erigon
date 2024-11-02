package app

import (
	"fmt"
	"reflect"
	"sync"
)

var typeInitFunctions = []func(){}
var typeInitMutex = sync.Mutex{}

func LocalTypeInit(localIdInitialiser func(govalue interface{}) TypeId,
	publicIdInitialiser func(domainValue interface{}, id []byte, idVersion Version) (TypeId, error)) {
	typeInitMutex.Lock()
	if NewLocalTypeId == nil {
		NewLocalTypeId = localIdInitialiser
		for _, initFunction := range typeInitFunctions {
			initFunction()
		}
		typeInitFunctions = []func(){}
	}
	typeInitMutex.Unlock()
}

var NewLocalTypeId func(govalue interface{}) TypeId

var typeIds = map[string]TypeId{}
var typeIdsMutex = &sync.RWMutex{}

func MustNewId(domain Domain, id ident, idVersion Version) TypeId {
	typeid, err := NewTypeId(domain, id, idVersion)

	if err != nil {
		panic(fmt.Sprintf("Can't create new type id: %s", err))
	}

	return typeid
}

func NewTypeId(domain Domain, id ident, idVersion Version) (TypeId, error) {

	base, err := NewId(domain, id /*idVersion*/)

	if err != nil {
		return nil, err
	}

	typeId := &typeId{base}
	value := typeId.String()

	typeIdsMutex.RLock()
	if registeredId, ok := typeIds[value]; ok {
		typeIdsMutex.RUnlock()
		return registeredId, nil
	}

	typeIdsMutex.RUnlock()
	typeIdsMutex.Lock()

	if registeredId, ok := typeIds[value]; ok {
		typeIdsMutex.Unlock()
		return registeredId, nil
	}

	typeIds[value] = typeId

	typeIdsMutex.Unlock()
	return typeId, nil
}

type TypeId interface {
	Id
	AssignableTo(typeId TypeId) bool
	TypeInfo() TypeInfo
}

type TypeInfo interface {
	Name() string
	LocalType() reflect.Type
}

type Typed interface {
	TypeId() TypeId
}

type typeId struct {
	Id
}

func (id *typeId) AssignableTo(typeId TypeId) bool {
	return id.CompareTo(typeId) == 0
}

func (id *typeId) TypeInfo() TypeInfo {
	return nil
}

func RegisterId(typeid TypeId) {
	typeIdsMutex.Lock()
	typeIds[typeid.String()] = typeid
	typeIdsMutex.Unlock()
}

type TypeArray []TypeId

func (a TypeArray) Len() int {
	return len(a)
}

func (a TypeArray) String() string {
	argTypes := make([]string, a.Len())
	for i, argType := range a {
		argTypes[i] = argType.String()
	}

	return fmt.Sprintf("%s", argTypes)
}

func (a TypeArray) Type(at interface{}) TypeId {
	return a[at.(int)]
}

type TypeMap map[string]TypeId

func (m TypeMap) Len() int {
	return len(m)
}

func (m TypeMap) String() string {
	argTypes := make([]string, m.Len())
	var i = 0
	for argName, argType := range m {
		argTypes[i] = fmt.Sprintf("%s=%s", argName, argType.String())
		i++
	}

	return fmt.Sprintf("%s", argTypes)
}

func (m TypeMap) Type(at interface{}) TypeId {
	return m[at.(string)]
}

type ArgTypes interface {
	Len() int
	String() string
	Type(at interface{}) TypeId
}

type Args interface {
	Arg(key interface{}) interface{}
	Len() int
}

type ArgArray []interface{}

func (a ArgArray) Arg(at interface{}) interface{} {
	return a[at.(int)]
}

func (a ArgArray) Len() int {
	return len(a)
}

type ArgMap map[string]interface{}

func (m ArgMap) Arg(at interface{}) interface{} {
	return m[at.(string)]
}

func (m ArgMap) Len() int {
	return len(m)
}

var LocalTypeDomain Domain = nil //TODO domain.Wrap(domain.MustDecodeId(encodertype.Base62, cri.EncodedSchemeapp.LocalTypeDomain, "0", nil, nil))
var Trace = false

var typeids = map[reflect.Type]TypeId{}
var typeidsMutex = &sync.RWMutex{}

func TypeIdOf(govalue interface{}) TypeId {
	var err error
	var gotype reflect.Type
	var ok bool

	if typeid, ok := govalue.(*GoTypeId); ok {
		return typeid
	}

	if gotype, ok = govalue.(reflect.Type); !ok {
		gotype = reflect.TypeOf(govalue)
	}

	if gotype == nil {
		return nil
	}

	if gotype.Kind() == reflect.Ptr {
		gotype = gotype.Elem()
	}

	typeidsMutex.RLock()
	id, ok := typeids[gotype]
	typeidsMutex.RUnlock()

	if !ok {
		typeidsMutex.Lock()
		id, err = newIdFor(gotype)

		if err != nil {
			typeidsMutex.Unlock()
			panic(fmt.Sprintf("Can't create type id for:%+v\n", err))
		}
		typeids[gotype] = id
		typeidsMutex.Unlock()
	}

	return id
}

type GoTypeId struct {
	Id
	typeInfo TypeInfo
}

func (ti *GoTypeId) TypeInfo() TypeInfo {
	return ti.typeInfo
}

func (ti *GoTypeId) AssignableTo(typeId TypeId) bool {
	if assignableId, ok := typeId.(*GoTypeId); ok {
		thisType := ti.typeInfo.LocalType()
		assignableType := assignableId.typeInfo.LocalType()

		if Trace {
			fmt.Printf("Go Type Assign %v->%v (%v)\n", thisType, assignableType,
				thisType.AssignableTo(assignableType) ||
					(thisType.Kind() == reflect.Ptr && (assignableType.Kind() != reflect.Ptr &&
						thisType.AssignableTo(reflect.PtrTo(assignableType)))) ||
					((assignableType.Kind() == reflect.Ptr || assignableType.Kind() == reflect.Interface) &&
						(thisType.Kind() != reflect.Ptr && reflect.PtrTo(thisType).AssignableTo(assignableType))))
		}

		if thisType.AssignableTo(assignableType) {
			return true
		}

		if thisType.Kind() == reflect.Ptr {
			return (assignableType.Kind() != reflect.Ptr &&
				thisType.AssignableTo(reflect.PtrTo(assignableType)))
		}

		if assignableType.Kind() == reflect.Ptr || assignableType.Kind() == reflect.Interface {
			return (thisType.Kind() != reflect.Ptr &&
				reflect.PtrTo(thisType).AssignableTo(assignableType))
		}
	}

	return false
}

func newIdFor(gotype reflect.Type) (TypeId, error) {

	typeName := gotype.Name()

	if pkgPath := gotype.PkgPath(); len(pkgPath) > 0 {
		typeName = fmt.Sprintf("%s.%s", pkgPath, typeName)
	}

	base, err := NewId(LocalTypeDomain, typeName)

	if err != nil {
		return nil, err
	}

	id := &GoTypeId{base, NewTypeInfo(gotype)}

	RegisterId(id)

	return id, nil
}

type typeInfo struct {
	localType reflect.Type
}

func NewTypeInfo(localType reflect.Type) TypeInfo {
	return &typeInfo{localType}
}

func (info *typeInfo) Name() string {
	if info.localType != nil {
		return info.localType.Name()
	}

	return ""
}

func (info *typeInfo) LocalType() reflect.Type {
	return info.localType
}

func TypeIdValues(typeIds interface{}) []TypeId {
	s := reflect.ValueOf(typeIds)

	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}

	var values []TypeId
	for i := 0; i < s.NumField(); i++ {
		if value, ok := s.Field(i).Interface().(TypeId); ok {
			values = append(values, value)
		}
	}
	return values
}
