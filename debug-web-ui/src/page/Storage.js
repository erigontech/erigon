import React from 'react';
import { Col, Container, Row } from 'react-bootstrap';
import LookupStorageForm from '../components/LookupStorageForm';

const StoragePage = ({ api }) => (
  <Container fluid className="mt-1">
    <Row>
      <Col>
        <h1>Storage</h1>
      </Col>
    </Row>
    <Row>
      <Col>
        <LookupStorageForm api={api} />
      </Col>
    </Row>
  </Container>
);

export default StoragePage;
