package tritonhttp

import "time"

type Response struct {
	Proto      string // e.g. "HTTP/1.1"
	StatusCode int    // e.g. 200
	StatusText string // e.g. "OK"

	// Headers stores all headers to write to the response.
	Headers map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	// Hint: you might need this to handle the "Connection: Close" requirement
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK(req *Request) {
	res.Proto = responseProto
	res.StatusCode = statusOK
	res.Headers = make(map[string]string)
	res.Headers["Date"] = FormatTime(time.Now())
	if req.Close {
		res.Headers["Connection"] = "close"
	}

}

func (res *Response) HandleFileNotFound(req *Request) {
	res.Proto = responseProto
	res.StatusCode = statusFileNotFound
	res.Headers = make(map[string]string)
	res.Headers["Date"] = FormatTime(time.Now())
	if req.Close {
		res.Headers["Connection"] = "close"
	}

}

func (res *Response) HandleBadRequest() {
	res.Proto = responseProto
	res.StatusCode = statusBadRequest
	res.Headers = make(map[string]string)
	res.Headers["Connection"] = "close"
	res.Headers["Date"] = FormatTime(time.Now())

}
