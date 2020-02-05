import React, { useState } from 'react'

import Form from 'react-bootstrap/Form'
import Button from 'react-bootstrap/Button'
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { Table } from 'react-bootstrap';

const LookupAccountForm = () => {
    const [accountID, setAccountID] = useState(123);

    return (
        <div>
            <TextField accountID={accountID} onClick={setAccountID} />
            <hr />
            { accountID && <DetailsForm accountID={accountID} /> }
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
                <Form.Group controlId="formBasicEmail">
                    <Form.Label>Account ID</Form.Label>
                    <Form.Control type="text"
                                  placeholder="Account ID"
                                  value={this.state.value}
                                  onChange={this.handleChange} />
                </Form.Group>
                <Button variant="primary" type="submit" onClick={this.handleSubmit}>Find</Button>
            </Form>
        );
    }
}

class DetailsForm extends React.Component {
    constructor(props) {
        super(props)
        this.state = {account: undefined}
    }

    componentDidMount() {
        // TODO: make an API call
    }

    render () { 
        return (
            <div>
                <Row>
                    <Col>
                        <h1>Search Result</h1>
                    </Col>
                </Row>
                <Row>
                    <Col>
                        <Table>
                            <Table.Row>
                                <Table.Col>
                                    ID
                                </Table.Col>
                                <Table.Col>
                                    <code>{this.props.accountID}</code>
                                </Table.Col>
                            </Table.Row>
                        </Table>
                    </Col>
                </Row>
            </div>
        );
    }
}

export default LookupAccountForm;