import React from 'react';

import {Col, Container, Nav, Row} from 'react-bootstrap';
import API from './utils/API.js'
import ErrorCatcher from './components/ErrorCatcher.js'
import {BrowserRouter as Router, Link, NavLink, Redirect, Route, Switch} from 'react-router-dom';
import AccountsPage from './page/Accounts';
import StorageTombstonesPage from './page/StorageTombstonesPage';
import {ReactComponent as Logo} from './logo.svg';
import './App.css';
import StoragePage from './page/Storage';
import RemoteDBForm from './components/RemoteDBForm';

const api = new API('http://localhost:8080')
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
    return (
        <ErrorCatcher>
            <Router>
                <Container fluid>
                    <Row>
                        <Col className="border-right p-0 sidebar min-vh-100" xs={3} md={2} lg={1}>
                            <Nav className="flex-column sticky-top">
                                <div className="mb-2 pb-1 border-bottom">
                                    <Link to="/">
                                        <Logo alt="Logo" sidth="120" height="120" className="d-block w-100"/>
                                    </Link>
                                    <div className="d-flex justify-content-center h4">Scope</div>
                                </div>
                                {sidebar.map((el, i) =>
                                    <NavLink key={i} to={el.url}
                                             className="pl-2 mb-2 font-weight-light">{el.label}
                                        <div className="active-pointer"/>
                                    </NavLink>
                                )}
                                <RemoteDBForm api={api}/>
                            </Nav>
                        </Col>
                        <Col xs={9} md={10} lg={11}>
                            <Switch>
                                <Redirect exact strict from="/" to="/accounts"/>
                                <Route exact path="/accounts">
                                    <AccountsPage api={api}/>
                                </Route>
                                <Route path="/storage">
                                    <StoragePage api={api}/>
                                </Route>
                                <Route path="/storage-tombstones">
                                    <StorageTombstonesPage api={api}/>
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
