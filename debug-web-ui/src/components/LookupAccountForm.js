import React, { useState } from 'react'

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Table } from 'react-bootstrap';

import ErrorForm from './ErrorForm.js';
import SearchField from './SearchField.js';


const loadAccount = (id, api) => {
    return api.lookupAccount(id)
}

const LookupAccountForm = ({api}) => {
    const [state, setState] = useState({account: undefined, error: undefined, loading: false});

    const lookupSuccess = (response) => setState({account: response.data, error: undefined});
    const lookupFail = (error) => {
        if (error && error.response && error.response.status === 404) {
            setState({account: undefined, error: error})
        } else {
            setState(() => { throw error })
        }
    }

    return (
        <div>
            <SearchField placeholder="lookup by id or hash"
                         onClick={(id) => loadAccount(id, api)
                                            .then(lookupSuccess)
                                            .catch(lookupFail)} />
            <hr />
            {state.account && <DetailsForm account={state.account} />}
            {state.error && <ErrorForm message={"Account not found"} />}
        </div>
    );
}

const DetailsForm = ({account}) => (
    <Row>
        <Col>
            <Table size="sm">
                <thead>
                    <tr>
                        <th><strong>Account</strong></th>
                        <th></th>
                    </tr>
                </thead>
                <tbody>
                    <TableRow name="balance" value={account.balance} />
                    <TableRow name="nonce" value={account.nonce} />
                    <TableRow name="code hash" value={account.code_hash} />
                    <TableRow name="storage hash" value={account.root_hash} />
                    <TableRow name="incarnation (turbo-geth)" value={account.implementation.incarnation} />
                </tbody>
            </Table>
        </Col>
    </Row>
);

const TableRow = ({name, value}) => (
    <tr>
        <td>{name}</td>
        <td><code>{value}</code></td>
    </tr>
);

export default LookupAccountForm;