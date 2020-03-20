import React, {useState} from 'react';

import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button'
import {InputGroup} from 'react-bootstrap';
import Modal from 'react-bootstrap/Modal';

const get = (api, setState) => {
    setState({host: '', port: '', loading: true});
    const lookupSuccess = (response) => setState({host: response.data.host, port: response.data.port, loading: false});
    const lookupFail = (error) => {
        setState({host: '', port: '', loading: false})

        setState(() => {
            throw error
        })
    }
    return api.getRemoteDB().then(lookupSuccess).catch(lookupFail);
}

const set = (host, port, api, setState) => {
    setState({host: host, port: port, loading: true});

    const lookupSuccess = () => setState({host: host, port: port, loading: false});
    const lookupFail = (error) => {
        setState({host: host, port: port, loading: true});

        setState(() => {
            throw error
        })
    }
    return api.setRemoteDB(host, port).then(lookupSuccess).catch(lookupFail);
}

const RemoteDBForm = ({api}) => {
    const [state, setState] = useState({host: '', port: ''});
    const [show, setShow] = useState(false);

    const handleHostChange = (event) => {
        const host = event.target.value;
        setState((prev) => {
            return {host: host, port: prev.port};
        });
    }

    const handlePortChange = (event) => {
        const port = event.target.value;
        setState((prev) => {
            return {host: prev.host, port: port};
        });
    }

    const handleSubmit = (event) => {
        event.preventDefault();
        set(state.host, state.port, api, setState)
        setShow(false)
    }

    const handleClick = () => {
        setShow(true)
        get(api, setState)
    }

    return (
        <div className="mt-3">
            <Button variant="outline-secondary" size="sm"
                    className="w-100 rounded-0 text-break"
                    onClick={handleClick}>
                Remote DB<br/>
                {state.host && state.host + ':' + state.port}
            </Button>
            <Modal show={show} onHide={() => setShow(false)}>
                <Modal.Header>
                    <Modal.Title>Remote DB</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form onSubmit={handleSubmit}>
                        <InputGroup className="mb-1" size="sm">
                            <Input label="Host" value={state.host} onChange={handleHostChange}/>
                        </InputGroup>
                        <InputGroup className="mb-1" size="sm">
                            <Input label="Port" value={state.port} onChange={handlePortChange}/>
                            <InputGroup.Append>
                                <Button variant="outline-primary" type="submit">Button</Button>
                            </InputGroup.Append>
                        </InputGroup>

                    </Form>
                </Modal.Body>
            </Modal>
        </div>

    );
}

const Input = ({label, value, onChange,}) => (
    <React.Fragment>
        <InputGroup.Prepend>
            <InputGroup.Text id="addon{label}">{label}</InputGroup.Text>
        </InputGroup.Prepend>
        <Form.Control
            type="text"
            placeholder="{Label}"
            aria-describedby="addon{label}"
            name="{label}"
            value={value || ''}
            onChange={onChange}/>
    </React.Fragment>
)

export default RemoteDBForm;