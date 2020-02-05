import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css';

import Container from 'react-bootstrap/Container'
import Row from 'react-bootstrap/Row'
import Col from 'react-bootstrap/Col'

import LookupAccountForm from './components/LookupAccountForm.js'
import API from './utils/API.js'

function App() {
  const api = new API('localhost:8080')
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
            <LookupAccountForm api={api}/>
          </Col>
        </Row>
      </Container>
    </div>
  );
}

export default App;
