package tritonhttp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// VirtualHosts contains a mapping from host name to the docRoot path
	// (i.e. the path to the directory to serve static files from) for
	// all virtual hosts that this server supports
	VirtualHosts map[string]string
}

const (
	responseProto = "HTTP/1.1"

	statusOK           = 200
	statusBadRequest   = 400
	statusFileNotFound = 404
)

var statusText = map[int]string{
	statusOK:           "OK",
	statusBadRequest:   "Bad Request",
	statusFileNotFound: "Not Found",
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {

	//Step1 : Validate all docRoots - not needed, already validate inside the main method

	//Step2: create your listen socket and spawn off goroutines per incoming client
	// server should now start to listen on the configured address
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	fmt.Println("Listening on", ln.Addr())

	// making sure the listener is closed when we exit
	defer func() {
		err = ln.Close()
		if err != nil {
			fmt.Println("error in closing listener", err)
		}
	}()

	// keep accepting connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		fmt.Println("accepted connection", conn.RemoteAddr())
		go s.HandleConnection(conn)
	}

}

// HandleConnection reads requests from the accepted conn and handles them.
func (s *Server) HandleConnection(conn net.Conn) {
	br := bufio.NewReader(conn)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			log.Printf("Failed to set timeout for connection %v", conn)
			_ = conn.Close()
			return
		}

		// Read next request from the client
		req, err, byteReceived := ReadRequest(br)

		//Handle EOF
		if errors.Is(err, io.EOF) {
			conn.Close()
			return

		}
		// make sure there is no timeout
		if err, ok := err.(net.Error); ok && err.Timeout() {
			if byteReceived {
				log.Printf("Connection to %v timed out during the read request", conn.RemoteAddr())
				res := &Response{}
				res.HandleBadRequest()
				err := res.Write(conn)
				if err != nil {
					fmt.Println("err writing the response back to client: ", err)
					conn.Close()
					return
				}
				conn.Close()
				return
			}
			conn.Close()
			return

		}
		// make sure we have a valid request start line and a valid host
		if err != nil {
			res := &Response{}
			res.HandleBadRequest()
			err := res.Write(conn)
			if err != nil {
				fmt.Println("err writing the response back to client: ", err)
				conn.Close()
				return
			}
			conn.Close()
			return
		}

		// Handle valid request
		log.Printf("Handle good request: %v", req)
		res := s.HandleGoodRequest(req)
		log.Printf("What is my response: %v", res)
		err = res.Write(conn)
		if err != nil {
			fmt.Println("err writing the response back to client: ", err)
			conn.Close()
			return
		}
		//FIXME
		if req.Close {
			log.Printf("need to close the connection!!!!")
			conn.Close()
			return
		}

	}
}

func (s *Server) HandleGoodRequest(req *Request) (res *Response) {
	res = &Response{}
	res.HandleOK(req)
	res.Request = req
	// if req.Close {
	// 	res.Headers["Connection"] = "close"
	// }

	//date
	//last modified

	//fetch the file
	fullPath, contentType, err := s.getFullPath(req.Host, req.URL)
	if err != nil {
		// invalid url request
		res.HandleBadRequest()
		return res

	}

	_, length, err := readFileAndGetLength(fullPath)
	if err != nil {
		// cannot find the fullPath, 404 error
		res.HandleFileNotFound(req)
		//there is no need to include connection close header in the response for 404 if there is no specify in the request header
		return res

	}
	res.FilePath = fullPath
	//content type
	res.Headers["Content-Type"] = contentType
	//content length
	res.Headers["Content-Length"] = length
	//last modified
	file, err := os.Stat(fullPath)
	if err != nil {
		fmt.Println("error checking the file")
		return res
	}
	res.Headers["Last-Modified"] = FormatTime(file.ModTime())

	return res
}

func readFileAndGetLength(fullPath string) (body []byte, length string, err error) {
	//FIXME!!!
	//open the file
	//absPath, _ := filepath.Abs(fullPath)
	//fmt.Println("what is my abspath: ", absPath)
	//fullPath = "/Users/songmingjie/Desktop/CSE224/proj2-MingjieSong/" + "docroot_dirs/htdocs1/index.html"
	file, err := os.Open(fullPath)
	if err != nil {
		fmt.Println("cannot find the file based on file path: ", err)
		return nil, "", err
	}
	//read from file in order to count the total bytes
	reader := bufio.NewReader(file)
	totalByte := 0
	for {
		record := make([]byte, 100)
		n, err := reader.Read(record)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}

		if err == io.EOF {
			break
		}
		totalByte = totalByte + n

	}
	file.Close()
	fmt.Println("sucess open the file!!!!!! ")
	fmt.Println("byte number that reads in ", totalByte)
	//return nil, "378"
	return nil, strconv.Itoa(totalByte), nil
}
func (s *Server) getFullPath(host string, url string) (fullPath string, contentType string, err error) {

	url = filepath.Clean(url)
	//check if the char is / and there is only one char
	if len(url) == 1 && url == "/" {
		url = "index.html"
		contentType = MIMETypeByExtension(".html")
	} else if len(url) > 1 && url[0:1] == "/" { //check if the first char is /
		if url[len(url)-1:] == "/" { //check if the last char is / --> append index.html at the end
			url = url[1:] + "index.html"
			contentType = MIMETypeByExtension(".html")
		} else {
			slashSplits := strings.Split(url, "/")
			dotSplits := strings.Split(slashSplits[len(slashSplits)-1], ".")
			contentType = MIMETypeByExtension("." + dotSplits[len(dotSplits)-1])
			url = url[1:]
		}
	} else {
		// all invalid url request
		// 400 error
		return "", "", fmt.Errorf("user request url is invalid ")

	}

	//fmt.Println("what is url ", url)
	//fmt.Println("what is host ", host)
	//if the first char is / and last char is not /  -> strings.Split(url, "/")

	//filePath := filepath.Join("docroot_dirs/", s.VirtualHosts[host], "/", url)
	//filePath := "docroot_dirs/" + s.VirtualHosts[host] + "/" + url
	//hostDoc := s.VirtualHosts[host]
	filePath := s.VirtualHosts[host] + "/" + url
	//log.Println("getFullPath test", filePath, "   ", contentType)
	return filePath, contentType, nil

}

func ReadRequest(br *bufio.Reader) (req *Request, err error, byteReceived bool) {
	req = &Request{}
	byteReceived = false

	// Read start line
	line, err := ReadLine(br)
	if err != nil {
		return nil, err, byteReceived
	}

	fields, err := parseRequestLine(line)
	if err != nil || fields[0] != "GET" || fields[1] == "" || fields[2] != responseProto { // do we need to check field[1] ??
		return nil, badStringError("malformed start line", line), byteReceived
	}
	byteReceived = true
	req.Method = fields[0]
	req.URL = fields[1]
	req.Proto = fields[2]

	// read all headers
	req.Headers = make(map[string]string)
	for {
		line, err := ReadLine(br)
		if err != nil {
			return nil, err, byteReceived
		}
		if line == "" {
			// This marks header end
			break
		}
		fmt.Println("Read headers from request", line)
		pair := strings.SplitN(line, ":", 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf(" Missing header value "), byteReceived

		}
		pair[0] = strings.ReplaceAll(pair[0], " ", "")
		pair[1] = strings.ReplaceAll(pair[1], " ", "")

		if pair[0] == "" || pair[1] == "" {
			return nil, fmt.Errorf(" Missing header value "), byteReceived
		}

		req.Headers[CanonicalHeaderKey(pair[0])] = pair[1]

		if strings.TrimSpace(pair[0]) == "Host" {
			req.Host = strings.TrimSpace(pair[1])
		}
		if strings.TrimSpace(pair[0]) == "Connection" {
			req.Close = true
		}

	}
	// there is no host or host value is empty, 400 bad request
	val, ok := req.Headers["Host"]
	if !ok || strings.TrimSpace(val) == "" {
		return nil, fmt.Errorf(" Missing host in the header "), byteReceived
	}

	return req, nil, byteReceived
}

// parseRequestLine parses "GET /foo HTTP/1.1" into its individual parts.
func parseRequestLine(line string) ([]string, error) {
	fields := strings.SplitN(line, " ", 3)
	if len(fields) != 3 {
		return nil, fmt.Errorf("could not parse the request line, got fields %v", fields)
	}
	return fields, nil
}

func badStringError(what, val string) error {
	return fmt.Errorf("%s %q", what, val)
}

func (res *Response) Write(w io.Writer) error {

	bw := bufio.NewWriter(w)

	statusLine := fmt.Sprintf("%v %v %v\r\n", res.Proto, res.StatusCode, statusText[res.StatusCode])
	if _, err := bw.WriteString(statusLine); err != nil {
		return err
	}
	//write key value pairs (header) response back to client over tcp
	//FIXME : key needed to be written in sorted order
	keys := make([]string, 0, len(res.Headers))
	for key := range res.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		line := fmt.Sprintf("%v: %v\r\n", key, res.Headers[key])
		if _, err := bw.WriteString(line); err != nil {
			return err
		}

	}
	if _, err := bw.WriteString("\r\n"); err != nil {
		return err
	}
	/*
		for key, element := range res.Headers {
			line := fmt.Sprintf("%v: %v\r\n", key, element)
			if _, err := bw.WriteString(line); err != nil {
				return err
			}

		} */

	if res.StatusCode == 200 {
		//write body if there is any
		file, err := os.Open(res.FilePath)
		if err != nil {
			fmt.Println("cannot find the file based on file path: ", err)
		}
		// read from file and write to client
		reader := bufio.NewReader(file)
		for {
			record := make([]byte, 100)
			n, err := reader.Read(record)
			if err != nil && err != io.EOF {
				log.Fatal(err)
			}

			if err == io.EOF {
				break
			}
			bw.Write(record[:n])

		}
		file.Close()

	}

	if err := bw.Flush(); err != nil {
		return err
	}
	return nil
}

// ReadLine reads a single line ending with "\r\n" from br,
// striping the "\r\n" line end from the returned string.
// If any error occurs, data read before the error is also returned.
// You might find this function useful in parsing requests.
func ReadLine(br *bufio.Reader) (string, error) {
	var line string
	for {
		s, err := br.ReadString('\n')
		line += s
		// Return the error
		if err != nil {
			return line, err
		}
		// Return the line when reaching line end
		if strings.HasSuffix(line, "\r\n") {
			// Striping the line end
			line = line[:len(line)-2]
			return line, nil
		}
	}
}
