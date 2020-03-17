import React from 'react';
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Container from "react-bootstrap/Container";
import LookupAccountForm from "../components/LookupAccountForm";

const AccountsPage = ({api}) => (
    <Container fluid className="mt-1">
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
)

export default  AccountsPage;