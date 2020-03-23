import React, {useState} from 'react'

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import {Spinner, Table} from 'react-bootstrap';

import SearchField from './SearchField.js';

const search = (prefix, api, setState) => {
    setState({hashes: undefined, loading: true});

    const lookupSuccess = (response) => setState({hashes: response.data, loading: false});
    const lookupFail = (error) => {
        setState({hashes: undefined, loading: false})

        setState(() => {
            throw error
        })
    }
    
    return api.lookupStorageTombstones(prefix).then(lookupSuccess).catch(lookupFail);
}

const LookupStorageTombstonesForm = ({api}) => {
    const [state, setState] = useState({hashes: undefined, loading: false});

    return (
        <div>
            {state.loading && <Spinner animation="border"/>}
            {!state.loading && <SearchField placeholder="lookup by prefix"
                                            onClick={(prefix) => search(prefix, api, setState)}/>}
            {state.hashes && <Details hashes={state.hashes}/>}
        </div>
    );
}

const Details = ({hashes}) => (
    <Row>
        <Col>
            <Table size="sm" borderless>
                <thead>
                <tr>
                    <th><strong>Prefix</strong></th>
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
    const {prefix,  hideStorage} = item

    return (
        <tr>
            <td className="text-monospace">
                {prefix}
            </td>
            <td className={hideStorage ? '' : 'bg-danger'}>
                {hideStorage ? 'yes' : 'no'}
            </td>
        </tr>
    );
};

export default LookupStorageTombstonesForm;