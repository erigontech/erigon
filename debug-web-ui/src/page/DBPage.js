import React, { useState } from 'react';
import { Button, Col, Container, Row, Spinner, Table } from 'react-bootstrap';

const DBPage = ({ api }) => {
  return (
    <Container fluid className="mt-1">
      <Row>
        <Col>
          <h1>DB</h1>
        </Col>
      </Row>
      <Row>
        <Col>
          <DBSize api={api} />
          <br />
          <DBBucketsStat api={api} />
        </Col>
      </Row>
    </Container>
  );
};

const loadDBSize = (api, setState) => {
  setState({ data: undefined, loading: true });
  const onSuccess = (response) => setState({ data: response.data, loading: false });
  const onFail = (error) => {
    setState({ data: undefined, loading: false });

    setState(() => {
      throw error;
    });
  };
  return api.dbSize().then(onSuccess).catch(onFail);
};

const DBSize = ({ api }) => {
  const [state, setState] = useState({ data: undefined, loading: false });

  return (
    <div>
      <Button onClick={() => loadDBSize(api, setState)}>Get DB Size</Button>
      {state.loading && <Spinner animation="border" />}
      {state.data && <div>DB size is: {state.data}</div>}
    </div>
  );
};

const loadDBBucketsStat = (api, setState) => {
  setState({ data: undefined, loading: true });
  const onSuccess = (response) => {
    setState({ data: response.data, loading: false });
  };
  const onFail = (error) => {
    setState({ data: undefined, loading: false });

    setState(() => {
      throw error;
    });
  };
  return api.dbBucketsStat().then(onSuccess).catch(onFail);
};

const DBBucketsStat = ({ api }) => {
  const [state, setState] = useState({ data: undefined, loading: false });

  return (
    <div>
      <Button onClick={() => loadDBBucketsStat(api, setState)}>Get Buckets Stats</Button>
      {state.loading && <Spinner animation="border" />}
      {state.data && <StatTable data={state.data} />}
    </div>
  );
};

const StatTable = ({ data }) => {
  const keys = Object.keys(data);
  const firstRow = data[keys[0]];
  return (
    <Table size="sm" borderless className="table-responsive">
      <thead>
        <StatHead stat={firstRow} />
      </thead>
      <tbody>
        {keys.map((bucket, i) => (
          <StatRow key={i} bucket={bucket} stat={data[bucket]} />
        ))}
      </tbody>
    </Table>
  );
};

const StatHead = ({ stat }) => (
  <tr>
    <th>Bucket</th>
    {Object.keys(stat).map((name, i) => (
      <th key={i}>{name}</th>
    ))}
  </tr>
);

const StatRow = ({ bucket, stat }) => (
  <tr>
    <td>{bucket}</td>
    {Object.keys(stat).map((name, i) => (
      <td key={i}>{stat[name]}</td>
    ))}
  </tr>
);

export default DBPage;
