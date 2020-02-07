import React from 'react';

const ErrorForm = ({error}) => {
    let message = error.message;
    if (error && error.response && error.response.status === 404) {
        message = "Not Found";
    }

    return (
        <code>{message || "Unknown error"}</code>
    );
}

export default ErrorForm;