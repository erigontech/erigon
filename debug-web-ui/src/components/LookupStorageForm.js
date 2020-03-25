import React, { useState } from 'react';

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Spinner, Table } from 'react-bootstrap';

import SearchField from './SearchField.js';

const search = (prefix, api, setState) => {
  setState({ data: undefined, loading: true });
  const lookupSuccess = (response) => setState({ data: response.data, loading: false });
  const lookupFail = (error) => {
    setState({ data: undefined, loading: false });

    setState(() => {
      throw error;
    });
  };
  return api.lookupStorage(prefix).then(lookupSuccess).catch(lookupFail);
};

const LookupStorageForm = ({ api }) => {
  const [state, setState] = useState({ data: undefined, loading: false });

  return (
    <div>
      {state.loading && <Spinner animation="border" />}
      {!state.loading && (
        <SearchField placeholder="lookup by prefix" onClick={(prefix) => search(prefix, api, setState)} />
      )}
      {state.data && <Details data={state.data} />}
    </div>
  );
};

const Details = ({ data }) => (
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
          {data.map((item, i) => (
            <TableRow key={i} item={item} />
          ))}
        </tbody>
      </Table>
    </Col>
  </Row>
);

const TableRow = ({ item }) => (
  <tr>
    <td className="text-monospace">{item.prefix}</td>
    <td className="text-monospace">{item.value}</td>
  </tr>
);

export default LookupStorageForm;
