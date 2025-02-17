import * as fs from "fs"
import {JsonRpcProvider, Transaction, TransactionResponse} from "ethers"

const args = new Map<string, string>()

process.argv.slice(2).forEach(a => {
    const nameValue = a.split("=")
    args.set(nameValue[0], nameValue[1])
})

if (args.size != 4) {
    throw new Error(`
incorrect usage, please provide:
 --rpcUrl=<rpcUrl>
 --txnHash=<txnHash>
 --traceConfig=<traceConfig> 
 --outputFilePath=<outputFilePath>`
    )
}

const rpcUrlInput = args.get("--rpcUrl")
if (!rpcUrlInput) {
    throw new Error("incorrect usage, please provide --<rpcUrl>")
}

const txnHashInput = args.get("--txnHash")
if (!txnHashInput) {
    throw new Error("incorrect usage, please provide --<txnHash>")
}

const traceConfigInput = args.get("--traceConfig")
if (!traceConfigInput) {
    throw new Error("incorrect usage, please provide --<traceConfig>")
}

const outputFilePath = args.get("--outputFilePath")
if (!outputFilePath) {
    throw new Error("incorrect usage, please provide --<outputFilePath>")
}

generateTest(rpcUrlInput, txnHashInput, JSON.parse(traceConfigInput))
    .then(testCase => {
        const json = JSON.stringify(testCase, null, 2)
        fs.writeFile(outputFilePath, json, {flag: "w+"}, (err) => {
            if (err != null) {
                console.error(err)
            }
        })
    })
    .catch(console.error)

class TestCase {
    constructor(
        public genesis: Genesis,
        public context: Context,
        public input: any,
        public tracerConfig: any,
        public result: any
    ) {
    }
}

async function generateTest(rpcUrl: string, txnHash: string, traceConfig: any): Promise<TestCase> {
    const provider = new JsonRpcProvider(rpcUrl)
    const txnResponse = await provider.getTransaction(txnHash).catch(err => {
        throw err
    })
    if (txnResponse == null) {
        throw new Error("null txnHash returned from provider")
    }

    const context = await generateContext(txnResponse)
    const genesis = await generateGenesis(provider, txnHash, parseInt(context.number) - 1)
    const result = await generateResult(provider, txnHash, traceConfig)
    const input = Transaction.from(txnResponse).serialized
    return Promise.resolve(new TestCase(genesis, context, input, traceConfig.tracerConfig, result))
}

class Genesis {
    constructor(
        public alloc: any,
        public config: any,
        public gasLimit: string, // required attribute for marshalling genesis struct at consumer end
        public difficulty: string, // required attribute for marshalling genesis struct at consumer end
    ) {
    }
}

async function generateGenesis(provider: JsonRpcProvider, txnHash: string, parentBlockNum: number): Promise<Genesis> {
    const alloc: any = await debugTraceTransaction(provider, txnHash, {"tracer": "prestateTracer"}).catch(err => {
        throw err
    })
    for (const key in alloc) {
        if ('nonce' in alloc[key]) {
            alloc[key].nonce = alloc[key].nonce.toString()
        }
    }

    const nodeInfo = await provider.send("admin_nodeInfo", []).catch(err => {
        throw err
    })

    const parentBlock = await provider.getBlock(parentBlockNum).catch(err => {
        throw err
    })
    if (parentBlock == null) {
        throw new Error("null parentBlock returned from provider")
    }

    const config = nodeInfo.protocols.eth.config
    // nullify as it causes issues with marshalling at consumer side
    // problem is that JSON.parse and JSON.stringify convert large numbers
    // above Number.MAX_VALUE into scientific notation
    // and consumers don't expect that when marshalling
    config.terminalTotalDifficulty = null

    return Promise.resolve(new Genesis(
        alloc,
        config,
        parentBlock.gasLimit.toString(),
        parentBlock.difficulty.toString(),
    ))
}

class Context {
    constructor(
        public number: string,
        public difficulty: string,
        public timestamp: string,
        public gasLimit: string,
        public baseFeePerGas: string | null,
        public miner: string,
    ) {
    }
}

async function generateContext(txnResponse: TransactionResponse): Promise<Context> {
    const block = await txnResponse.getBlock().catch(err => {
        throw err
    })
    if (block == null) {
        throw new Error("null block returned from provider")
    }

    return Promise.resolve(new Context(
        block.number.toString(),
        block.difficulty.toString(),
        block.timestamp.toString(),
        block.gasLimit.toString(),
        bigIntToStringOrNull(block.baseFeePerGas),
        block.miner,
    ))
}

async function generateResult(provider: JsonRpcProvider, txnHash: string, traceConfig: any): Promise<any> {
    let res = await debugTraceTransaction(provider, txnHash, traceConfig).catch(err => {
        throw err
    })
    delete res["time"]
    return Promise.resolve(res)
}

async function debugTraceTransaction(provider: JsonRpcProvider, txnHash: string, traceConfig: any): Promise<any> {
    return provider.send("debug_traceTransaction", [txnHash, traceConfig])
}

function bigIntToStringOrNull(baseFeePerGas: bigint | null): string | null {
    if (baseFeePerGas == null) {
        return null
    }

    return baseFeePerGas.toString()
}
