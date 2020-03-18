import React from 'react';

import {Nav, Navbar} from 'react-bootstrap';
import API from './utils/API.js'
import ErrorCatcher from './components/ErrorCatcher.js'
import {BrowserRouter as Router, NavLink, Redirect, Route, Switch} from 'react-router-dom';
import AccountsPage from './page/Accounts';
import StorageTombstones from './page/StorageTombstones';

function App() {
    const api = new API('http://localhost:8080')
    return (
        <ErrorCatcher>
            <Router>
                <Navbar bg="dark" variant="dark">
                    <Navbar.Brand href="#home">Turbo-Geth Debug Utility</Navbar.Brand>
                    <Navbar.Toggle aria-controls="basic-navbar-nav"/>
                    <Navbar.Collapse id="basic-navbar-nav">
                        <Nav className="mr-auto" variant="pills">
                            <Nav.Item>
                                <NavLink to="/accounts" className="nav-link">Accounts</NavLink>
                            </Nav.Item>
                            <Nav.Item>
                                <NavLink to="/storage-tombstones" className="nav-link">Storage Tombstones</NavLink>
                            </Nav.Item>
                        </Nav>
                    </Navbar.Collapse>
                </Navbar>
                <Switch>
                    <Redirect exact strict from="/" to="/accounts"/>
                    <Route exact path="/accounts">
                        <AccountsPage api={api}/>
                    </Route>
                    <Route path="/storage-tombstones">
                        <StorageTombstones api={api}/>
                    </Route>
                </Switch>
            </Router>
        </ErrorCatcher>
    );
}

export default App;
