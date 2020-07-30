import React, { useState } from 'react';

import { Button, Form, Modal } from 'react-bootstrap';

const RemoteSidebar = ({ api, restHost, restPort, onApiChange }) => {
  return (
    <React.Fragment>
      <RestApiForm host={restHost} port={restPort} onApiChange={onApiChange} />
      <RemoteDBForm api={api} />
    </React.Fragment>
  );
};

const RestApiForm = ({ host, port, onApiChange }) => {
  const [show, setShow] = useState(false);

  const handleSubmit = (e) => {
    e.preventDefault();
    setShow(false);
    let form = e.target;
    onApiChange({ host: form.elements.host.value, port: form.elements.port.value });
  };

  const handleClick = (e) => {
    e.preventDefault();
    setShow(true);
  };

  return (
    <div className="mb-2 font-weight-light text-break">
      <a href="/rest-api" className="nav-link px-2" onClick={handleClick}>
        Rest API
        <br />
        {host && host + ':' + port}
      </a>
      <ModalWindow title="Rest API" show={show} onHide={() => setShow(false)}>
        <Form onSubmit={handleSubmit}>
          <Input label="Host" defaultValue={host} />
          <Input label="Port" defaultValue={port} />
          <Button type="submit">Submit</Button>
        </Form>
      </ModalWindow>
    </div>
  );
};

const get = (api, setHost, setPort) => {
  const lookupSuccess = (response) => {
    setHost(response.data.host);
    setPort(response.data.port);
  };
  const lookupFail = (error) => {
    setHost(() => {
      throw error;
    });
  };
  return api.getPrivateAPI().then(lookupSuccess).catch(lookupFail);
};

const set = (host, port, api, setHost, setPort) => {
  const lookupSuccess = () => {
    setHost(host);
    setPort(port);
  };
  const lookupFail = (error) => {
    setHost(() => {
      throw error;
    });
  };
  return api.setPrivateAPI(host, port).then(lookupSuccess).catch(lookupFail);
};

const RemoteDBForm = ({ api }) => {
  const [host, setHost] = useState('');
  const [port, setPort] = useState('');
  const [show, setShow] = useState(false);

  const handleSubmit = (e) => {
    e.preventDefault();
    e.stopPropagation();
    const form = e.target;
    set(form.elements.host.value, form.elements.port.value, api, setHost, setPort);
    setShow(false);
  };

  const handleClick = (e) => {
    e.preventDefault();
    setShow(true);
    get(api, setHost, setPort);
  };

  return (
    <div className="pl-2 mb-2 font-weight-light text-break">
      <a href="/private-api" onClick={handleClick}>
        Private Api
        <br />
        {host && host + ':' + port}
      </a>
      <ModalWindow title="Private Api" show={show} onHide={() => setShow(false)}>
        <Form onSubmit={handleSubmit}>
          <Input label="Host" defaultValue={host} />
          <Input label="Port" defaultValue={port} />
          <Button type="submit">Submit</Button>
        </Form>
      </ModalWindow>
    </div>
  );
};

const Input = ({ label, ...props }) => (
  <Form.Group controlId={label.toLowerCase()}>
    <Form.Label>{label}</Form.Label>
    <Form.Control
      type="text"
      placeholder={label}
      aria-describedby={'addon' + label.toLowerCase()}
      name={label.toLowerCase()}
      {...props}
    />
  </Form.Group>
);

const ModalWindow = ({ children, title, ...props }) => (
  <Modal {...props}>
    <Modal.Header>
      <Modal.Title>{title}</Modal.Title>
    </Modal.Header>
    <Modal.Body>{children}</Modal.Body>
  </Modal>
);

export default RemoteSidebar;
