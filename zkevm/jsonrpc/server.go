package jsonrpc

const (
	// APIEth represents the eth API prefix.
	APIEth = "eth"
	// APINet represents the net API prefix.
	APINet = "net"
	// APIDebug represents the debug API prefix.
	APIDebug = "debug"
	// APIZKEVM represents the zkevm API prefix.
	APIZKEVM = "zkevm"
	// APITxPool represents the txpool API prefix.
	APITxPool = "txpool"
	// APIWeb3 represents the web3 API prefix.
	APIWeb3 = "web3"

	wsBufferSizeLimitInBytes = 1024
)

/*

// Server is an API backend to handle RPC requests
type Server struct {
	config     Config
	handler    *Handler
	srv        *http.Server
	wsSrv      *http.Server
	wsUpgrader websocket.Upgrader
}

// NewServer returns the JsonRPC server
func NewServer(
	cfg Config,
	p types.PoolInterface,
	s types.StateInterface,
	storage storageInterface,
	apis map[string]bool,
) *Server {
	s.PrepareWebSocket()
	handler := newJSONRpcHandler()

	if _, ok := apis[APIEth]; ok {
		ethEndpoints := newEthEndpoints(cfg, p, s, storage)
		handler.registerService(APIEth, ethEndpoints)
	}

	if _, ok := apis[APINet]; ok {
		netEndpoints := &NetEndpoints{cfg: cfg}
		handler.registerService(APINet, netEndpoints)
	}

	if _, ok := apis[APIZKEVM]; ok {
		zkEVMEndpoints := &ZKEVMEndpoints{state: s, config: cfg}
		handler.registerService(APIZKEVM, zkEVMEndpoints)
	}

	if _, ok := apis[APITxPool]; ok {
		txPoolEndpoints := &TxPoolEndpoints{}
		handler.registerService(APITxPool, txPoolEndpoints)
	}

	if _, ok := apis[APIDebug]; ok {
		debugEndpoints := &DebugEndpoints{state: s}
		handler.registerService(APIDebug, debugEndpoints)
	}

	if _, ok := apis[APIWeb3]; ok {
		web3Endpoints := &Web3Endpoints{}
		handler.registerService(APIWeb3, web3Endpoints)
	}

	srv := &Server{
		config:  cfg,
		handler: handler,
	}
	return srv
}

// Start initializes the JSON RPC server to listen for request
func (s *Server) Start() error {
	metrics.Register()

	if s.config.WebSockets.Enabled {
		go s.startWS()
	}

	return s.startHTTP()
}

// startHTTP starts a server to respond http requests
func (s *Server) startHTTP() error {
	if s.srv != nil {
		return fmt.Errorf("server already started")
	}

	address := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Errorf("failed to create tcp listener: %v", err)
		return err
	}

	mux := http.NewServeMux()

	lmt := tollbooth.NewLimiter(s.config.MaxRequestsPerIPAndSecond, nil)
	mux.Handle("/", tollbooth.LimitFuncHandler(lmt, s.handle))

	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: s.config.ReadTimeoutInSec * time.Second,
		ReadTimeout:       s.config.ReadTimeoutInSec * time.Second,
		WriteTimeout:      s.config.WriteTimeoutInSec * time.Second,
	}
	log.Infof("http server started: %s", address)
	if err := s.srv.Serve(lis); err != nil {
		if err == http.ErrServerClosed {
			log.Infof("http server stopped")
			return nil
		}
		log.Errorf("closed http connection: %v", err)
		return err
	}
	return nil
}

// startWS starts a server to respond WebSockets connections
func (s *Server) startWS() {
	log.Infof("starting websocket server")

	if s.wsSrv != nil {
		log.Errorf("websocket server already started")
		return
	}

	address := fmt.Sprintf("%s:%d", s.config.Host, s.config.WebSockets.Port)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Errorf("failed to create tcp listener: %v", err)
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleWs)

	s.wsSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: s.config.ReadTimeoutInSec * time.Second,
		ReadTimeout:       s.config.ReadTimeoutInSec * time.Second,
		WriteTimeout:      s.config.WriteTimeoutInSec * time.Second,
	}
	s.wsUpgrader = websocket.Upgrader{
		ReadBufferSize:  wsBufferSizeLimitInBytes,
		WriteBufferSize: wsBufferSizeLimitInBytes,
	}
	log.Infof("websocket server started: %s", address)
	if err := s.wsSrv.Serve(lis); err != nil {
		if err == http.ErrServerClosed {
			log.Infof("websocket server stopped")
			return
		}
		log.Errorf("closed websocket connection: %v", err)
		return
	}
}

// Stop shutdown the rpc server
func (s *Server) Stop() error {
	if s.srv != nil {
		if err := s.srv.Shutdown(context.Background()); err != nil {
			return err
		}

		if err := s.srv.Close(); err != nil {
			return err
		}
		s.srv = nil
	}

	if s.wsSrv != nil {
		if err := s.wsSrv.Shutdown(context.Background()); err != nil {
			return err
		}

		if err := s.wsSrv.Close(); err != nil {
			return err
		}
		s.wsSrv = nil
	}

	return nil
}

func (s *Server) handle(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

	if (*req).Method == "OPTIONS" {
		// TODO(pg): need to count it in the metrics?
		return
	}

	if req.Method == "GET" {
		// TODO(pg): need to count it in the metrics?
		_, err := w.Write([]byte("zkEVM JSON RPC Server"))
		if err != nil {
			log.Error(err)
		}
		return
	}

	if req.Method != "POST" {
		err := errors.New("method " + req.Method + " not allowed")
		s.handleInvalidRequest(w, err)
		return
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		s.handleInvalidRequest(w, err)
		return
	}

	single, err := s.isSingleRequest(data)
	if err != nil {
		s.handleInvalidRequest(w, err)
		return
	}

	start := time.Now()
	var respLen int
	if single {
		respLen = s.handleSingleRequest(req, w, data)
	} else {
		respLen = s.handleBatchRequest(req, w, data)
	}
	metrics.RequestDuration(start)
	combinedLog(req, start, http.StatusOK, respLen)
}

func (s *Server) isSingleRequest(data []byte) (bool, types.Error) {
	x := bytes.TrimLeft(data, " \t\r\n")

	if len(x) == 0 {
		return false, types.NewRPCError(types.InvalidRequestErrorCode, "Invalid json request")
	}

	return x[0] == '{', nil
}

func (s *Server) handleSingleRequest(httpRequest *http.Request, w http.ResponseWriter, data []byte) int {
	defer metrics.RequestHandled(metrics.RequestHandledLabelSingle)
	request, err := s.parseRequest(data)
	if err != nil {
		handleError(w, err)
		return 0
	}
	req := handleRequest{Request: request, HttpRequest: httpRequest}
	response := s.handler.Handle(req)

	respBytes, err := json.Marshal(response)
	if err != nil {
		handleError(w, err)
		return 0
	}

	_, err = w.Write(respBytes)
	if err != nil {
		handleError(w, err)
		return 0
	}
	return len(respBytes)
}

func (s *Server) handleBatchRequest(httpRequest *http.Request, w http.ResponseWriter, data []byte) int {
	defer metrics.RequestHandled(metrics.RequestHandledLabelBatch)
	requests, err := s.parseRequests(data)
	if err != nil {
		handleError(w, err)
		return 0
	}

	responses := make([]types.Response, 0, len(requests))

	for _, request := range requests {
		req := handleRequest{Request: request, HttpRequest: httpRequest}
		response := s.handler.Handle(req)
		responses = append(responses, response)
	}

	respBytes, _ := json.Marshal(responses)
	_, err = w.Write(respBytes)
	if err != nil {
		log.Error(err)
		return 0
	}
	return len(respBytes)
}

func (s *Server) parseRequest(data []byte) (types.Request, error) {
	var req types.Request

	if err := json.Unmarshal(data, &req); err != nil {
		return types.Request{}, types.NewRPCError(types.InvalidRequestErrorCode, "Invalid json request")
	}

	return req, nil
}

func (s *Server) parseRequests(data []byte) ([]types.Request, error) {
	var requests []types.Request

	if err := json.Unmarshal(data, &requests); err != nil {
		return nil, types.NewRPCError(types.InvalidRequestErrorCode, "Invalid json request")
	}

	return requests, nil
}

func (s *Server) handleInvalidRequest(w http.ResponseWriter, err error) {
	defer metrics.RequestHandled(metrics.RequestHandledLabelInvalid)
	handleError(w, err)
}

func (s *Server) handleWs(w http.ResponseWriter, req *http.Request) {
	// CORS rule - Allow requests from anywhere
	s.wsUpgrader.CheckOrigin = func(r *http.Request) bool { return true }

	// Upgrade the connection to a WS one
	wsConn, err := s.wsUpgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Error(fmt.Sprintf("Unable to upgrade to a WS connection, %s", err.Error()))

		return
	}

	// Defer WS closure
	defer func(ws *websocket.Conn) {
		err = ws.Close()
		if err != nil {
			log.Error(fmt.Sprintf("Unable to gracefully close WS connection, %s", err.Error()))
		}
	}(wsConn)

	log.Info("Websocket connection established")
	for {
		msgType, message, err := wsConn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure, websocket.CloseAbnormalClosure) {
				log.Info("Closing WS connection gracefully")
			} else {
				log.Error(fmt.Sprintf("Unable to read WS message, %s", err.Error()))
				log.Info("Closing WS connection with error")
			}

			s.handler.RemoveFilterByWsConn(wsConn)

			break
		}

		if msgType == websocket.TextMessage || msgType == websocket.BinaryMessage {
			go func() {
				resp, err := s.handler.HandleWs(message, wsConn)
				if err != nil {
					log.Error(fmt.Sprintf("Unable to handle WS request, %s", err.Error()))
					_ = wsConn.WriteMessage(msgType, []byte(fmt.Sprintf("WS Handle error: %s", err.Error())))
				} else {
					_ = wsConn.WriteMessage(msgType, resp)
				}
			}()
		}
	}
}

func handleError(w http.ResponseWriter, err error) {
	log.Error(err)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		log.Error(err)
	}
}

func rpcErrorResponse(code int, message string, err error) (interface{}, types.Error) {
	return rpcErrorResponseWithData(code, message, nil, err)
}

func rpcErrorResponseWithData(code int, message string, data *[]byte, err error) (interface{}, types.Error) {
	if err != nil {
		log.Errorf("%v:%v", message, err.Error())
	} else {
		log.Error(message)
	}
	return nil, types.NewRPCErrorWithData(code, message, data)
}

func combinedLog(r *http.Request, start time.Time, httpStatus, dataLen int) {
	log.Infof("%s - - %s \"%s %s %s\" %d %d \"%s\" \"%s\"",
		r.RemoteAddr,
		start.Format("[02/Jan/2006:15:04:05 -0700]"),
		r.Method,
		r.URL.Path,
		r.Proto,
		httpStatus,
		dataLen,
		r.Host,
		r.UserAgent(),
	)
}

*/
