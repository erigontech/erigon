package main

type BatchByNumber struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      string `json:"id"`
	Result  struct {
		AccInputHash string `json:"accInputHash"`
		BatchL2Data  string `json:"batchL2Data"`
		Blocks       []struct {
			ParentHash       string `json:"parentHash"`
			Sha3Uncles       string `json:"sha3Uncles"`
			Miner            string `json:"miner"`
			StateRoot        string `json:"stateRoot"`
			TransactionsRoot string `json:"transactionsRoot"`
			ReceiptsRoot     string `json:"receiptsRoot"`
			LogsBloom        string `json:"logsBloom"`
			Difficulty       string `json:"difficulty"`
			TotalDifficulty  string `json:"totalDifficulty"`
			Size             string `json:"size"`
			Number           string `json:"number"`
			GasLimit         string `json:"gasLimit"`
			GasUsed          string `json:"gasUsed"`
			Timestamp        string `json:"timestamp"`
			ExtraData        string `json:"extraData"`
			MixHash          string `json:"mixHash"`
			Nonce            string `json:"nonce"`
			Hash             string `json:"hash"`
			Transactions     []any  `json:"transactions"`
			Uncles           []any  `json:"uncles"`
			BlockInfoRoot    string `json:"blockInfoRoot"`
			GlobalExitRoot   string `json:"globalExitRoot"`
		} `json:"blocks"`
		Closed              bool   `json:"closed"`
		Coinbase            string `json:"coinbase"`
		GlobalExitRoot      string `json:"globalExitRoot"`
		LocalExitRoot       string `json:"localExitRoot"`
		MainnetExitRoot     string `json:"mainnetExitRoot"`
		Number              string `json:"number"`
		RollupExitRoot      string `json:"rollupExitRoot"`
		SendSequencesTxHash string `json:"sendSequencesTxHash"`
		StateRoot           string `json:"stateRoot"`
		Timestamp           string `json:"timestamp"`
		Transactions        any    `json:"transactions"`
		VerifyBatchTxHash   string `json:"verifyBatchTxHash"`
	} `json:"result"`
}

type ZkevmFullBlock struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      string `json:"id"`
	Result  struct {
		ParentHash       string `json:"parentHash"`
		Sha3Uncles       string `json:"sha3Uncles"`
		Miner            string `json:"miner"`
		StateRoot        string `json:"stateRoot"`
		TransactionsRoot string `json:"transactionsRoot"`
		ReceiptsRoot     string `json:"receiptsRoot"`
		LogsBloom        string `json:"logsBloom"`
		Difficulty       string `json:"difficulty"`
		TotalDifficulty  string `json:"totalDifficulty"`
		Size             string `json:"size"`
		Number           string `json:"number"`
		GasLimit         string `json:"gasLimit"`
		GasUsed          string `json:"gasUsed"`
		Timestamp        string `json:"timestamp"`
		ExtraData        string `json:"extraData"`
		MixHash          string `json:"mixHash"`
		Nonce            string `json:"nonce"`
		Hash             string `json:"hash"`
		Transactions     []struct {
			Nonce            string `json:"nonce"`
			GasPrice         string `json:"gasPrice"`
			Gas              string `json:"gas"`
			To               string `json:"to"`
			Value            string `json:"value"`
			Input            string `json:"input"`
			V                string `json:"v"`
			R                string `json:"r"`
			S                string `json:"s"`
			Hash             string `json:"hash"`
			From             string `json:"from"`
			BlockHash        string `json:"blockHash"`
			BlockNumber      string `json:"blockNumber"`
			TransactionIndex string `json:"transactionIndex"`
			Type             string `json:"type"`
			Receipt          struct {
				CumulativeGasUsed string `json:"cumulativeGasUsed"`
				LogsBloom         string `json:"logsBloom"`
				Logs              []any  `json:"logs"`
				Status            string `json:"status"`
				TransactionHash   string `json:"transactionHash"`
				TransactionIndex  string `json:"transactionIndex"`
				BlockHash         string `json:"blockHash"`
				BlockNumber       string `json:"blockNumber"`
				GasUsed           string `json:"gasUsed"`
				From              string `json:"from"`
				To                string `json:"to"`
				ContractAddress   any    `json:"contractAddress"`
				Type              string `json:"type"`
				EffectiveGasPrice string `json:"effectiveGasPrice"`
				TransactionL2Hash string `json:"transactionL2Hash"`
			} `json:"receipt"`
			L2Hash string `json:"l2Hash"`
		} `json:"transactions"`
		Uncles []any `json:"uncles"`
	} `json:"result"`
}
