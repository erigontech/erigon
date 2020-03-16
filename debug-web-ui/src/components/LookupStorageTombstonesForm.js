import React, {useState} from 'react'

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import {Spinner, Table} from 'react-bootstrap';

import SearchField from './SearchField.js';

const LookupStorageTombstonesForm = ({api}) => {
    const [state, setState] = useState({hashes: undefined, loading: false});

    const search = (prefix) => {
        setState({hashes: undefined, error: undefined, loading: true})
        api.lookupIntermediateHashes(prefix)
            .then(lookupSuccess)
            .catch(lookupFail)
    }
    const lookupSuccess = (response) => setState({hashes: response.data, loading: false});
    const lookupFail = (error) => {
        setState({hashes: undefined, loading: false})
        if (error) throw error
    }

    return (
        <div>
            {state.loading && <Spinner animation="border"/>}
            {!state.loading && <SearchField placeholder="lookup by prefix" onClick={search}/>}

            <hr/>
            {state.hashes && <DetailsForm hashes={state.hashes}/>}
        </div>
    );
}

const DetailsForm = ({hashes}) => (
    <Row>
        <Col>
            <Table size="sm">
                <thead>
                <tr>
                    <th><strong>Prefix</strong></th>
                </tr>
                </thead>
                <tbody>
                {hashes.map((el, i) => <TableRow key={i} prefix={el}/>)}
                </tbody>
            </Table>
        </Col>
    </Row>
);

const TableRow = ({prefix, value}) => (
    <tr>
        <td><code>{prefix}</code></td>
    </tr>
);

export default LookupStorageTombstonesForm;