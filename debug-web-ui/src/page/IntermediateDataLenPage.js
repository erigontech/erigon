import React from 'react';

import { Col, Container, Row } from 'react-bootstrap';
import LookupIntermediateDataLenForm from "../components/LookupIntermediateDataLenForm";

const IntermediateDataLenPage = ({ api }) => (
  <Container fluid className="mt-1">
    <Row>
      <Col>
        <h1>Intermediate Data Len</h1>
      </Col>
    </Row>
    <Row>
      <Col xs={10}>
        <LookupIntermediateDataLenForm api={api} />
      </Col>
    </Row>
  </Container>
);

export default IntermediateDataLenPage;
