import React from 'react'

import Modal from 'react-bootstrap/Modal'
import Button from 'react-bootstrap/Button'

export default class ErrorCatcher extends React.Component {
    constructor(props) {
        super(props);
        this.state = { error: undefined}
        
        this.handleClose = this.handleClose.bind(this);
        this.handleReload = this.handleReload.bind(this);
    }

    static getDerivedStateFromError(error) {
        // Update state so the next render will show the fallback UI.
        return {error: error};
      }

    componentDidCatch(error, errorInfo) {
        this.setState({error: error})
    }

    handleClose(event) {
        this.setState({error: undefined});
    }

    handleReload(event) {
        // force reload
        window.location.reload();
    }

    render() {
        let show = this.state.error !== undefined;
        return (
            <div class={this.props.className}>
                {this.props.children}
                <Modal show={show} onHide={this.handleClose}>
                    <Modal.Header>
                        <Modal.Title>Unexpected Error</Modal.Title>
                    </Modal.Header>
                    <Modal.Body>
                        <code>{this.state.error && this.state.error.message}</code>
                    </Modal.Body>
                    <Modal.Footer>
                        <Button variant="secondary" onClick={this.handleClose}>Ignore</Button>
                        <Button variant="primary" onClick={this.handleReload}>Reload</Button>
                    </Modal.Footer>
                </Modal>
            </div>
        );
    }

}