import React, { useState } from 'react'

import Form from 'react-bootstrap/Form'
import Button from 'react-bootstrap/Button'
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Table } from 'react-bootstrap';

const LookupAccountForm = ({api}) => {
    const [accountID, setAccountID] = useState(undefined);

    return (
        <div>
            <TextField accountID={accountID} onClick={setAccountID} />
            <hr />
            { accountID && <DetailsForm accountID={accountID} api={api} /> }
        </div>
    );
}


class TextField extends React.Component {
    constructor(props) {
        super(props);
        this.state = {value: this.props.accountID};

        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
    }

    handleChange(event) {
        this.setState({value: event.target.value});
    }

    handleSubmit(event) {
        this.props.onClick(this.state.value);
        event.preventDefault()
    }

    render() {
        return (
            <Form>
                <Form.Row>
                    <Col>
                        <Form.Control type="text"
                                    placeholder="Account ID"
                                    value={this.state.value || ''}
                                    onChange={this.handleChange} />
                    </Col>
                    <Col>
                        <Button variant="primary" type="submit" onClick={this.handleSubmit}>Find</Button>
                    </Col>
                </Form.Row>
            </Form>
        );
    }
}

class DetailsForm extends React.Component {
    constructor(props) {
        super(props)
        this.state = {account: undefined, error: undefined}
    }

    componentDidMount() {
        this.props.api
            .lookupAccount(this.props.accountID)
            .then((response) => this.setState({account: response.data, error: undefined}))
            .catch((error) => this.setState({account: undefined, error: error}))
            // TODO: implement a catcher
    }


    render () { 

        console.log(JSON.stringify(this.state.account));

        if (!this.state.account && !this.state.error) {
            return (
                <div>loading...</div>
            );
        }

        if (this.state.error && this.state.error.response.status == 404) {
            return (
                <code>Account Not Found</code>
            )
        }

        if (this.state.error) {
            return (
                <code>{this.state.error.message || "Unknown error"}</code>
            )
        }

        return (
            <div>
                <Row>
                    <Col>
                        <h1>Account</h1>
                    </Col>
                </Row>
                <Row>
                    <Col>
                        <Table>
                            <tbody>
                                <TableRow name="balance" value={this.state.account.balance} />
                                <TableRow name="nonce" value={this.state.account.nonce} />
                                <TableRow name="code hash" value={this.state.account.code_hash} />
                                <TableRow name="storage hash" value={this.state.account.root_hash} />
                                <TableRow name="incarnation (turbo-geth)" value={this.state.account.implementation.incarnation} />
                            </tbody>
                        </Table>
                    </Col>
                </Row>
            </div>
        );
    }
}

const TableRow = ({name, value}) => (
    <tr>
        <td>{name}</td>
        <td><code>{value}</code></td>
    </tr>
);

export default LookupAccountForm;