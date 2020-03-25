import React, { useState } from 'react';

import { Col, Container, Nav, Row } from 'react-bootstrap';
import API from './utils/API.js';
import ErrorCatcher from './components/ErrorCatcher.js';
import { BrowserRouter as Router, Link, NavLink, Redirect, Route, Switch } from 'react-router-dom';
import AccountsPage from './page/Accounts';
import StorageTombstonesPage from './page/StorageTombstonesPage';
import { ReactComponent as Logo } from './logo.svg';
import './App.css';
import StoragePage from './page/Storage';
import RemoteSidebar from './components/RemoteSidebar';

const sidebar = [
  {
    url: '/accounts',
    label: 'Accounts',
  },
  {
    url: '/storage',
    label: 'Storage',
  },
  {
    url: '/storage-tombstones',
    label: 'Storage Tombs',
  },
];

function App() {
  const [host, setHost] = useState('localhost');
  const [port, setPort] = useState('8080');
  const onApiChange = (data) => {
    setHost(data.host);
    setPort(data.port);
  };

  const api = new API('http://' + host + ':' + port);

  return (
    <ErrorCatcher>
      <Router>
        <Container fluid>
          <Row>
            <Col className="border-right p-0 sidebar min-vh-100" xs={3} md={2} lg={1}>
              <Nav className="flex-column sticky-top">
                <div className="mb-2 pb-1 border-bottom">
                  <Link to="/">
                    <Logo alt="Logo" sidth="120" height="120" className="d-block w-100" />
                  </Link>
                  <div className="d-flex justify-content-center h4">Scope</div>
                </div>
                {sidebar.map((el, i) => (
                  <NavLink key={i} to={el.url} className="pl-2 mb-2 font-weight-light">
                    {el.label}
                    <div className="active-pointer" />
                  </NavLink>
                ))}
                <div className="mt-5 border-secondary border-top" />
                <RemoteSidebar api={api} restHost={host} restPort={port} onApiChange={onApiChange} />
              </Nav>
            </Col>
            <Col xs={9} md={10} lg={11}>
              <Switch>
                <Redirect exact strict from="/" to="/accounts" />
                <Route exact path="/accounts">
                  <AccountsPage api={api} />
                </Route>
                <Route path="/storage">
                  <StoragePage api={api} />
                </Route>
                <Route path="/storage-tombstones">
                  <StorageTombstonesPage api={api} />
                </Route>
              </Switch>
            </Col>
          </Row>
        </Container>
      </Router>
    </ErrorCatcher>
  );
}

export default App;
