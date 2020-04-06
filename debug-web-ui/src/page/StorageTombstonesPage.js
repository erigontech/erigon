import React from 'react';

import { Col, Container, Row } from 'react-bootstrap';
import LookupStorageTombstonesForm from '../components/LookupStorageTombstonesForm';
import StorageTombstonesIntegrityChecks from '../components/StorageTombstonesIntegrityChecks';

const StorageTombstonesPage = ({ api }) => (
  <Container fluid className="mt-1">
    <Row>
      <Col>
        <h1>Storage Tombstones</h1>
      </Col>
    </Row>
    <Row>
      <Col xs={10}>
        <LookupStorageTombstonesForm api={api} />
      </Col>
      <Col xs={2}>
        <StorageTombstonesIntegrityChecks api={api} />
      </Col>
    </Row>
  </Container>
);

export default StorageTombstonesPage;
