export default class API {
    constructor(baseURL) {
        this.baseURL = baseURL
    }

    endpoint(name) {
        return this.baseURL + name
    }

    lookupAccount(accountID) {
        return new Promise((resolve, reject) => {
            resolve({
                'id': accountID,
                'balance': 999,
                'nonce': 1,
                'rootHash': 'blah-root-hash',
                'incarnation': 1,
                'codeHash': 'blah-code-hash'
            })
        });
    }
}