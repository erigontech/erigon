import React, { useState } from 'react';

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Spinner, Table } from 'react-bootstrap';

import SearchField from './SearchField.js';

const search = (prefix, api, setState) => {
  setState({ hashes: undefined, loading: true });

  const lookupSuccess = (response) => setState({ hashes: response.data, loading: false });
  const lookupFail = (error) => {
    setState({ hashes: undefined, loading: false });

    setState(() => {
      throw error;
    });
  };

  return api.lookupIntermediateHashes(prefix).then(lookupSuccess).catch(lookupFail);
};

const LookupIntermediateHashForm = ({ api }) => {
  const [state, setState] = useState({ hashes: undefined, loading: false });

  return (
    <div>
      <SearchField
        placeholder="lookup by prefix"
        disabled={state.loading}
        onSubmit={(data) => search(data.search, api, setState)}
      />
      {state.loading && <Spinner animation="border" />}
      {state.hashes && <Details hashes={state.hashes} />}
    </div>
  );
};

const Details = ({ hashes }) => (
  <Row>
    <Col>
      <Table size="sm" borderless>
        <thead>
          <tr>
            <th>
              <strong>Key</strong>
            </th>
            <th>
              <strong>Value</strong>
            </th>
          </tr>
        </thead>
        <tbody>
          {hashes.map((item, i) => (
            <TableRow key={i} item={item} />
          ))}
        </tbody>
      </Table>
    </Col>
  </Row>
);

const TableRow = ({ item }) => {
  const { prefix, value } = item;

  return (
    <tr>
      <td className="text-monospace">{prefix}</td>
      <td className="text-monospace">{value}</td>
    </tr>
  );
};

export default LookupIntermediateHashForm;
