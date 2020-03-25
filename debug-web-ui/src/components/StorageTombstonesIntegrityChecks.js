import React, { useState } from 'react';

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Button, Spinner, Table } from 'react-bootstrap';

const load = (api, setState) => {
  const lookupSuccess = (response) => setState({ data: response.data, loading: false });
  const lookupFail = (error) => {
    setState({ data: undefined, loading: false });

    setState(() => {
      throw error;
    });
  };

  setState({ data: undefined, loading: true });
  return api.storageTombstonesIntegrityChecks().then(lookupSuccess).catch(lookupFail);
};

const StorageTombstonesIntegrityChecks = ({ api }) => {
  const [state, setState] = useState({ data: undefined, loading: false });

  return (
    <div>
      {state.loading && <Spinner animation="border" />}
      {!state.loading && (
        <Button size="sm" onClick={() => load(api, setState)}>
          Integrity Checks
        </Button>
      )}
      {state.data && <DetailsForm data={state.data} />}
    </div>
  );
};

const DetailsForm = ({ data }) => (
  <Row>
    <Col>
      <Table size="sm" borderless>
        <thead>
          <tr>
            <th>
              <strong>Check</strong>
            </th>
            <th>Result</th>
          </tr>
        </thead>
        <tbody>
          {data.map((el, i) => (
            <TableRow key={i} name={el.name} value={el.value} />
          ))}
        </tbody>
      </Table>
    </Col>
  </Row>
);

const TableRow = ({ name, value }) => (
  <tr>
    <td>{name}</td>
    <td>
      <code>{value}</code>
    </td>
  </tr>
);

export default StorageTombstonesIntegrityChecks;
