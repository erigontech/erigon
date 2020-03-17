import React, {useState} from 'react'

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import {Spinner, Table} from 'react-bootstrap';

import SearchField from './SearchField.js';

const search = (prefix, api, setState) => {
    return api.lookupIntermediateHashes(prefix);
}

const LookupStorageTombstonesForm = ({api}) => {
    const [state, setState] = useState({hashes: undefined, loading: false});

    const lookupSuccess = (response) => setState({hashes: response.data, loading: false});
    const lookupFail = (error) => {
        // console.debug(Object.keys(error), error.response.data)
        setState({hashes: undefined, loading: false})
        throw error
    }

    return (
        <div>
            {state.loading && <Spinner animation="border"/>}
            {!state.loading && <SearchField placeholder="lookup by prefix" onClick={
                (prefix) => {
                    setState({hashes: undefined, loading: true});

                    search(prefix, api, setState)
                        .then(lookupSuccess)
                        .catch(lookupFail)
                }}/>}
            <hr/>
            {state.hashes && <DetailsForm hashes={state.hashes}/>}
        </div>
    );
}

const DetailsForm = ({hashes}) => (
    <Row>
        <Col>
            <Table size="sm" borderless>
                <thead>
                <tr>
                    <th><strong>Prefix</strong></th>
                    <th><strong>Don't overlap other tomb</strong></th>
                    <th><strong>Hide storage</strong></th>
                </tr>
                </thead>
                <tbody>
                {hashes.map((item, i) => <TableRow key={i} item={item}/>)}
                </tbody>
            </Table>
        </Col>
    </Row>
);

const TableRow = ({item}) => {
    const {prefix, dontOverlapOtherTomb, hideStorage} = item

    return (
        <tr>
            <td className="text-monospace">
                {prefix}
            </td>
            <td className={dontOverlapOtherTomb ? '' : 'bg-danger'}>
                {dontOverlapOtherTomb ? 'yes' : 'no'}
            </td>
            <td className={hideStorage ? '' : 'bg-danger'}>
                {hideStorage ? 'yes' : 'no'}
            </td>
        </tr>
    );
};

export default LookupStorageTombstonesForm;