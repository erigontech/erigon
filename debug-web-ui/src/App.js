import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';

import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'

import LookupAccountForm from './components/LookupAccountForm.js'

function App() {
  return (
    <div className="App">
      <Container>
        <Row>
          <Col>
            <h1>TURBO-GETH DB Explorer</h1>
          </Col>
        </Row>
        <Row>
          <Col>
            <h3>Lookup Account</h3>
          </Col>
        </Row>
        <Row>
          <Col>
            <LookupAccountForm />
          </Col>
        </Row>
      </Container>
    </div>
  );
}

export default App;
