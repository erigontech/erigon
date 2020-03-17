import React from 'react';

import {Col, Container, Row} from 'react-bootstrap';
import LookupStorageTombstonesForm from '../components/LookupStorageTombstonesForm';

const StorageTombstones = ({api}) => (

    <Container fluid className="mt-1">
        <Row>
            <Col>
                <h1>Storage Tombstones</h1>
            </Col>
        </Row>
        <Row>
            <Col>
                <LookupStorageTombstonesForm api={api}/>
            </Col>
        </Row>
    </Container>
)

export default StorageTombstones;