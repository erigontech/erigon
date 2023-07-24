package exp

import (
	"bufio"
	"bytes"
	"net/http"
	"strings"
)

func makePrometheusCompatible(buffer *bytes.Buffer) *bytes.Buffer {
	scanner := bufio.NewScanner(buffer)
	buf := &bytes.Buffer{}
	ioBuf := NewIoWriterBuffer(buf)

	for scanner.Scan() {
		line := scanner.Text()
		var comment string
		if strings.Contains(line, "total") {
			comment = "# TYPE " + strings.Split(strings.Split(line, " ")[0], "{")[0] + " " + "counter\n"
		} else {
			comment = "# TYPE " + strings.Split(strings.Split(line, " ")[0], "{")[0] + " " + "gauge\n"
		}
		ioBuf.WriteString(comment + line + "\n")
	}

	return buf
}

type IoWriterBuffer struct {
	buffer *bytes.Buffer
}

func NewIoWriterBuffer(buffer *bytes.Buffer) *IoWriterBuffer {
	return &IoWriterBuffer{
		buffer: buffer,
	}
}

func (w *IoWriterBuffer) Header() http.Header {
	return nil
}

func (w *IoWriterBuffer) WriteHeader(statusCode int) {
	return
}

func (w *IoWriterBuffer) Write(data []byte) (int, error) {
	return w.buffer.Write(data)
}

func (w *IoWriterBuffer) WriteString(s string) (int, error) {
	bytes := []byte(s)
	return w.buffer.Write(bytes)
}
