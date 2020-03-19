import React from 'react';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Container from 'react-bootstrap/Container';
import LookupStorageForm from '../components/LookupStorageForm';

const StoragePage = ({api}) => (
    <Container fluid className="mt-1">
        <Row>
            <Col>
                <h1>Storage</h1>
            </Col>
        </Row>
        <Row>
            <Col>
                <LookupStorageForm api={api}/>
            </Col>
        </Row>
    </Container>
)

export default StoragePage;