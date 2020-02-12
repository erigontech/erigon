const axios = require('axios');

export default class API {
    constructor(baseURL) {
        this.baseURL = baseURL
    }

    endpoint(name) {
        return this.baseURL + name
    }

    lookupAccount(id) {
        return axios({
            url: this.endpoint('/api/v1/accounts/' + id),
            method: 'get',
        })
    }
}