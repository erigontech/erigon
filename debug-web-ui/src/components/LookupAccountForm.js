import React, { useState } from 'react'

import Form from 'react-bootstrap/Form'
import Button from 'react-bootstrap/Button'

const LookupAccountForm = () => {
    const [accountID, setAccountID] = useState(123);

    return (
        <div>
            <TextField accountID={accountID} onClick={setAccountID} />
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

const DetailsForm = ({accountID}) => (
    <div>
        ID = {accountID}
    </div>
)

export default LookupAccountForm;