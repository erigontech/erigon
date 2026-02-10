package rlp

//
//func DialTransport(ctx context.Context, rawUrl string, transport *http.Transport) (*Client, error) {
//	u, err := url.Parse(rawUrl)
//	if err != nil {
//		return nil, err
//	}
//
//	var rpcClient *Client
//	switch u.Scheme {
//	case "http", "https":
//		client := &http.Client{
//			Transport: transport,
//		}
//		rpcClient, err = DialHTTPWithClient(rawUrl, client)
//	case "ws", "wss":
//		rpcClient, err = DialWebsocket(ctx, rawUrl, "")
//	case "stdio":
//		return DialStdIO(ctx)
//	case "":
//		return DialIPC(ctx, rawUrl)
//	default:
//		return nil, fmt.Errorf("no known transport for scheme %q in URL %s", u.Scheme, rawUrl)
//	}
//	if err != nil {
//		return nil, err
//	}
//	return rpcClient, nil
//}
