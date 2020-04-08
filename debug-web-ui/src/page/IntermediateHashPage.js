import React from 'react';

import { Col, Container, Row } from 'react-bootstrap';
import LookupIntermediateHashForm from '../components/LookupIntermediateHashForm';

const IntermediateHashPage = ({ api }) => (
  <Container fluid className="mt-1">
    <Row>
      <Col>
        <h1>Intermediate Hash</h1>
      </Col>
    </Row>
    <Row>
      <Col xs={10}>
        <LookupIntermediateHashForm api={api} />
      </Col>
    </Row>
  </Container>
);

export default IntermediateHashPage;
