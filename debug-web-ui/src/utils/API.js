const axios = require('axios');

export default class API {
  constructor(baseURL) {
    this.baseURL = baseURL;
  }

  endpoint(name) {
    return this.baseURL + name;
  }

  getRemoteDB() {
    return axios({
      url: this.endpoint('/api/v1/remote-db/'),
      method: 'get',
    });
  }

  setRemoteDB(host, port) {
    return axios({
      url: this.endpoint('/api/v1/remote-db/'),
      method: 'post',
      params: {
        host: host,
        port: port,
      },
    });
  }

  lookupAccount(id) {
    return axios({
      url: this.endpoint('/api/v1/accounts/' + id),
      method: 'get',
    });
  }

  lookupStorage(prefix) {
    return axios({
      url: this.endpoint('/api/v1/storage/'),
      method: 'get',
      params: {
        prefix: prefix,
      },
    });
  }

  lookupIntermediateHashes(prefix) {
    return axios({
      url: this.endpoint('/api/v1/intermediate-hash/'),
      method: 'get',
      params: {
        prefix: prefix,
      },
    });
  }

  lookupIntermediateDataLen(prefix) {
    return axios({
      url: this.endpoint('/api/v1/intermediate-data-len/'),
      method: 'get',
      params: {
        prefix: prefix,
      },
    });
  }
}
