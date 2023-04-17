package jsonrpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"unicode"

	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

const (
	requiredReturnParamsPerFn = 2
)

type serviceData struct {
	sv      reflect.Value
	funcMap map[string]*funcData
}

type funcData struct {
	inNum int
	reqt  []reflect.Type
	fv    reflect.Value
	isDyn bool
}

func (f *funcData) numParams() int {
	return f.inNum - 1
}

type handleRequest struct {
	types.Request
	wsConn      *websocket.Conn
	HttpRequest *http.Request
}

// Handler manage services to handle jsonrpc requests
//
// Services are public structures containing public methods
// matching the name of the jsonrpc method.
//
// Services must be registered with a prefix to identify the
// service and its methods, for example a service registered
// with a prefix `eth` will have all the public methods exposed
// as eth_<methodName> through the json rpc server.
//
// Go public methods requires the first char of its name to be
// in uppercase, but the exposition of the method will consider
// it to lower case, for example a method `func MyMethod()`
// provided by the service registered with `eth` prefix will
// be triggered when the method eth_myMethod is specified
//
// the public methods must follow the conventions:
// - return interface{}, rpcError
// - if the method depend on a Web Socket connection, it must be the first parameters as f(*websocket.Conn)
// - parameter types must match the type of the data provided for the method
//
// check the `eth.go` file for more example on how the methods are implemented
type Handler struct {
	serviceMap map[string]*serviceData
}

func newJSONRpcHandler() *Handler {
	handler := &Handler{
		serviceMap: map[string]*serviceData{},
	}
	return handler
}

var connectionCounter = 0
var connectionCounterMutex sync.Mutex

// Handle is the function that knows which and how a function should
// be executed when a JSON RPC request is received
func (h *Handler) Handle(req handleRequest) types.Response {
	log := log.WithFields("method", req.Method, "requestId", req.ID)
	connectionCounterMutex.Lock()
	connectionCounter++
	connectionCounterMutex.Unlock()
	defer func() {
		connectionCounterMutex.Lock()
		connectionCounter--
		connectionCounterMutex.Unlock()
		log.Debugf("Current open connections %d", connectionCounter)
	}()
	log.Debugf("Current open connections %d", connectionCounter)
	log.Debugf("request params %v", string(req.Params))

	service, fd, err := h.getFnHandler(req.Request)
	if err != nil {
		return types.NewResponse(req.Request, nil, err)
	}

	inArgsOffset := 0
	inArgs := make([]reflect.Value, fd.inNum)
	inArgs[0] = service.sv

	requestHasWebSocketConn := req.wsConn != nil
	funcHasMoreThanOneInputParams := len(fd.reqt) > 1
	firstFuncParamIsWebSocketConn := false
	firstFuncParamIsHttpRequest := false
	if funcHasMoreThanOneInputParams {
		firstFuncParamIsWebSocketConn = fd.reqt[1].AssignableTo(reflect.TypeOf(&websocket.Conn{}))
		firstFuncParamIsHttpRequest = fd.reqt[1].AssignableTo(reflect.TypeOf(&http.Request{}))
	}
	if requestHasWebSocketConn && firstFuncParamIsWebSocketConn {
		inArgs[1] = reflect.ValueOf(req.wsConn)
		inArgsOffset++
	} else if firstFuncParamIsHttpRequest {
		// If in the future one endponit needs to have both a websocket connection and an http request
		// we will need to modify this code to properly handle it
		inArgs[1] = reflect.ValueOf(req.HttpRequest)
		inArgsOffset++
	}

	// check params passed by request match function params
	var testStruct []interface{}
	if err := json.Unmarshal(req.Params, &testStruct); err == nil && len(testStruct) > fd.numParams() {
		return types.NewResponse(req.Request, nil, types.NewRPCError(types.InvalidParamsErrorCode, fmt.Sprintf("too many arguments, want at most %d", fd.numParams())))
	}

	inputs := make([]interface{}, fd.numParams()-inArgsOffset)

	for i := inArgsOffset; i < fd.inNum-1; i++ {
		val := reflect.New(fd.reqt[i+1])
		inputs[i-inArgsOffset] = val.Interface()
		inArgs[i+1] = val.Elem()
	}

	if fd.numParams() > 0 {
		if err := json.Unmarshal(req.Params, &inputs); err != nil {
			return types.NewResponse(req.Request, nil, types.NewRPCError(types.InvalidParamsErrorCode, "Invalid Params"))
		}
	}

	output := fd.fv.Call(inArgs)
	if err := getError(output[1]); err != nil {
		log.Infof("failed call: [%v]%v. Params: %v", err.ErrorCode(), err.Error(), string(req.Params))
		return types.NewResponse(req.Request, nil, err)
	}

	var data []byte
	res := output[0].Interface()
	if res != nil {
		d, _ := json.Marshal(res)
		data = d
	}

	return types.NewResponse(req.Request, data, nil)
}

// HandleWs handle websocket requests
func (h *Handler) HandleWs(reqBody []byte, wsConn *websocket.Conn) ([]byte, error) {
	var req types.Request
	if err := json.Unmarshal(reqBody, &req); err != nil {
		return types.NewResponse(req, nil, types.NewRPCError(types.InvalidRequestErrorCode, "Invalid json request")).Bytes()
	}

	handleReq := handleRequest{
		Request: req,
		wsConn:  wsConn,
	}

	return h.Handle(handleReq).Bytes()
}

// RemoveFilterByWsConn uninstalls the filter attached to this websocket connection
func (h *Handler) RemoveFilterByWsConn(wsConn *websocket.Conn) {
	service, ok := h.serviceMap[APIEth]
	if !ok {
		return
	}

	ethEndpointsInterface := service.sv.Interface()
	if ethEndpointsInterface == nil {
		log.Errorf("failed to get ETH endpoint interface")
	}

	ethEndpoints := ethEndpointsInterface.(*EthEndpoints)
	if ethEndpoints == nil {
		log.Errorf("failed to get ETH endpoint instance")
		return
	}

	err := ethEndpoints.uninstallFilterByWSConn(wsConn)
	if err != nil {
		log.Errorf("failed to uninstall filter by web socket connection:, %v", err)
		return
	}
}

func (h *Handler) registerService(serviceName string, service interface{}) {
	st := reflect.TypeOf(service)
	if st.Kind() == reflect.Struct {
		panic(fmt.Sprintf("jsonrpc: service '%s' must be a pointer to struct", serviceName))
	}

	funcMap := make(map[string]*funcData)
	for i := 0; i < st.NumMethod(); i++ {
		mv := st.Method(i)
		if mv.PkgPath != "" {
			// skip unexported methods
			continue
		}

		name := lowerCaseFirst(mv.Name)
		funcName := serviceName + "_" + name
		fd := &funcData{
			fv: mv.Func,
		}
		var err error
		if fd.inNum, fd.reqt, err = validateFunc(funcName, fd.fv, true); err != nil {
			panic(fmt.Sprintf("jsonrpc: %s", err))
		}
		// check if last item is a pointer
		if fd.numParams() != 0 {
			last := fd.reqt[fd.numParams()]
			if last.Kind() == reflect.Ptr {
				fd.isDyn = true
			}
		}
		funcMap[name] = fd
	}

	h.serviceMap[serviceName] = &serviceData{
		sv:      reflect.ValueOf(service),
		funcMap: funcMap,
	}
}

func (h *Handler) getFnHandler(req types.Request) (*serviceData, *funcData, types.Error) {
	methodNotFoundErrorMessage := fmt.Sprintf("the method %s does not exist/is not available", req.Method)

	callName := strings.SplitN(req.Method, "_", 2) //nolint:gomnd
	if len(callName) != 2 {                        //nolint:gomnd
		return nil, nil, types.NewRPCError(types.NotFoundErrorCode, methodNotFoundErrorMessage)
	}

	serviceName, funcName := callName[0], callName[1]

	service, ok := h.serviceMap[serviceName]
	if !ok {
		log.Infof("Method %s not found", req.Method)
		return nil, nil, types.NewRPCError(types.NotFoundErrorCode, methodNotFoundErrorMessage)
	}
	fd, ok := service.funcMap[funcName]
	if !ok {
		return nil, nil, types.NewRPCError(types.NotFoundErrorCode, methodNotFoundErrorMessage)
	}
	return service, fd, nil
}

func validateFunc(funcName string, fv reflect.Value, isMethod bool) (inNum int, reqt []reflect.Type, err error) {
	if funcName == "" {
		err = fmt.Errorf("getBlockNumByArg cannot be empty")
		return
	}

	ft := fv.Type()
	if ft.Kind() != reflect.Func {
		err = fmt.Errorf("function '%s' must be a function instead of %s", funcName, ft)
		return
	}

	inNum = ft.NumIn()
	outNum := ft.NumOut()

	if outNum != requiredReturnParamsPerFn {
		err = fmt.Errorf("unexpected number of output arguments in the function '%s': %d. Expected 2", funcName, outNum)
		return
	}
	if !isRPCErrorType(ft.Out(1)) {
		err = fmt.Errorf("unexpected type for the second return value of the function '%s': '%s'. Expected '%s'", funcName, ft.Out(1), rpcErrType)
		return
	}

	reqt = make([]reflect.Type, inNum)
	for i := 0; i < inNum; i++ {
		reqt[i] = ft.In(i)
	}
	return
}

var rpcErrType = reflect.TypeOf((*types.Error)(nil)).Elem()

func isRPCErrorType(t reflect.Type) bool {
	return t.Implements(rpcErrType)
}

func getError(v reflect.Value) types.Error {
	if v.IsNil() {
		return nil
	}

	switch vt := v.Interface().(type) {
	case *types.RPCError:
		return vt
	default:
		return types.NewRPCError(types.DefaultErrorCode, "runtime error")
	}
}

func lowerCaseFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToLower(v)) + str[i+1:]
	}
	return ""
}
