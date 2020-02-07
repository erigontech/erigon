import React from 'react';

import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';

import LookupAccountForm from './components/LookupAccountForm.js'
import API from './utils/API.js'

function App() {
  const api = new API('http://localhost:8080')
  return (
    <div className="App">
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
      <Container>
        <Row>
          <Col>
            <h3>Accounts</h3>
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
