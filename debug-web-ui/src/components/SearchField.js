import React from 'react';

import Form from 'react-bootstrap/Form';
import Col from 'react-bootstrap/Col';
import Button from 'react-bootstrap/Button';

const SearchField = (props) => {
  const handleSubmit = (event) => {
    event.preventDefault();
    let data = {};
    const formData = new FormData(event.target);
    for (const [key, value] of formData.entries()) {
      data[key] = value;
    }
    props.onSubmit(data);
  };

  return (
    <Form onSubmit={handleSubmit} disabled={props.disabled}>
      <Form.Row>
        <Col>
          <Form.Control size="sm" name="search" placeholder={props.placeholder} />
        </Col>
        <Col>
          <Button disabled={props.disabled} variant="primary" type="submit" size="sm">
            Find
          </Button>
        </Col>
      </Form.Row>
      <Form.Row>
        <Col>{props.children}</Col>
      </Form.Row>
    </Form>
  );
};

export default SearchField;
