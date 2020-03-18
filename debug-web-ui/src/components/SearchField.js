import React from 'react';

import Form from 'react-bootstrap/Form';
import Col from 'react-bootstrap/Col'
import Button from 'react-bootstrap/Button'


class SearchField extends React.Component {
    constructor(props) {
        super(props);
        this.state = {value: ''};

        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
    }

    handleChange(event) {
        this.setState({value: event.target.value});
    }

    handleSubmit(event) {
        this.props.onClick(this.state.value);
        event.preventDefault();
    }

    render() {
        return (
            <Form onSubmit={this.handleSubmit}  >
                <Form.Row>
                    <Col>
                        <Form.Control type="text" size="sm"
                                    placeholder={this.props.placeholder}
                                    value={this.state.value || ''}
                                    onChange={this.handleChange} />
                    </Col>
                    <Col>
                        <Button variant="primary" type="submit" size="sm">Find</Button>
                    </Col>
                </Form.Row>
            </Form>
        );
    }
}

export default SearchField;