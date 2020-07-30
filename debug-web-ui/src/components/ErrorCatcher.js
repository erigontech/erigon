import React from 'react';

import Modal from 'react-bootstrap/Modal';
import Button from 'react-bootstrap/Button';

export default class ErrorCatcher extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: undefined, errorInfo: undefined };

    this.handleClose = this.handleClose.bind(this);
    this.handleReload = this.handleReload.bind(this);
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI.
    return { error: error };
  }

  componentDidCatch(error, errorInfo) {
    this.setState({ error: error, errorInfo: errorInfo });
  }

  handleClose(event) {
    this.setState({ error: undefined });
  }

  handleReload(event) {
    // force reload
    window.location.reload();
  }

  render() {
    let err = this.state.error;
    let show = err !== undefined;
    let info = this.state.errorInfo !== undefined ? this.state.errorInfo.componentStack : '';
    let details = process.env.NODE_ENV === 'development' ? info : '';

    let serverErrors = [];
    if (err !== undefined && err.response !== undefined) {
      if (Array.isArray(err.response.data)) {
        serverErrors = err.response.data;
      } else if (err.response.data.error !== undefined) {
        serverErrors = [err.response.data.error];
      }
    }

    return (
      <div className={this.props.className}>
        {this.props.children}
        <Modal show={show} onHide={this.handleClose} size="xl">
          <Modal.Header>
            <Modal.Title>Unexpected Error</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <code>Server Error: {serverErrors.join('<br/>')}</code>
            <br />
            <code>{this.state.error && this.state.error.message}</code>
            <pre>{details}</pre>
          </Modal.Body>
          <Modal.Footer>
            <Button variant="secondary" onClick={this.handleClose}>
              Ignore
            </Button>
            <Button variant="primary" onClick={this.handleReload}>
              Reload
            </Button>
          </Modal.Footer>
        </Modal>
      </div>
    );
  }
}
