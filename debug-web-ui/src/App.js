import React from 'react';

import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';

import LookupAccountForm from './components/LookupAccountForm.js'
import API from './utils/API.js'
import ErrorCatcher from './components/ErrorCatcher.js'

function App() {
  const api = new API('http://localhost:8080')
  return (
    <ErrorCatcher className="App">
        <Navbar bg="dark" variant="dark">
          <Navbar.Brand href="#home">Turbo-Geth Debug Utility</Navbar.Brand>
          <Navbar.Toggle aria-controls="basic-navbar-nav" />
          <Navbar.Collapse id="basic-navbar-nav">
            <Nav className="mr-auto" variant="pills" defaultActiveKey="/">
              <Nav.Item>
                <Nav.Link href="/">Accounts</Nav.Link>
              </Nav.Item>
            </Nav>
          </Navbar.Collapse>
        </Navbar>
        <Container style={{'margin-top': '1rem'}}>
          <Row>
            <Col>
              <h1>Lookup Accounts</h1>
            </Col>
          </Row>
          <Row>
            <Col>
              <LookupAccountForm api={api}/>
            </Col>
          </Row>
        </Container>
    </ErrorCatcher>
  );
}

export default App;
