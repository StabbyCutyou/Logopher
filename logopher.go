// Package logopher provides a way to communicate with LogStash over UDP
package logopher

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

const basicMessageFormat = "{\"@timestamp\":\"%s\", \"@version\":\"2\", \"message\":\"%s\", \"host\":\"%s\"}\n"

// UDPWriter represents an abstraction over the raw UDPConn and error handling
// for writing data to logstash via udp
type UDPWriter struct {
	socket        *net.UDPConn
	address       string
	enableLogging bool
}

// DialUDP createsa a new UDPWriter
func DialUDP(address string, enableLogging bool) (*UDPWriter, error) {
	writer := &UDPWriter{
		address:       address,
		enableLogging: enableLogging,
	}

	if err := writer.open(); err != nil {
		return nil, err
	}
	return writer, nil
}

// open will dial a connection to the remote endpoint
func (u *UDPWriter) open() error {
	udpAddr, err := net.ResolveUDPAddr("udp", u.address)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return err
	}
	u.socket = conn
	return err
}

// Close will immediately call close on the connection to the remote endpoint. You
// should not call this if other threads may be using the underlying socktet, unless
// you control it in a mutex of some kind.
func (u *UDPWriter) Close() error {
	return u.socket.Close()
}

// Reopen allows you to close and re-establish a connection to the existing Address
// without needing to create a whole new UDPWriter object
func (u *UDPWriter) Reopen() error {
	if err := u.Close(); err != nil {
		return err
	}

	if err := u.open(); err != nil {
		return err
	}

	return nil
}

// Log crafts a payload body, and writes it to logstash
func (u *UDPWriter) Log(msg string) (int, error) {
	host, _ := os.Hostname()
	data := fmt.Sprintf(basicMessageFormat, time.Now().String(), msg, host)
	return u.Write([]byte(data))
	//log.Printf(data)
	//return 0, nil
}

// Write writes the given string, plus a newline, to the LogStash server. If not
// all bytes can be written, Write will keep trying until the full message is
// delivered, or the connection is broken.
func (u *UDPWriter) Write(rawBytes []byte) (int, error) {
	toWriteLen := len(rawBytes)
	// Three conditions could have occured:
	// 1. There was an error
	// 2. Not all bytes were written
	// 3. Both 1 and 2

	// If there was an error, that should take handling precedence. If the connection
	// was closed, or is otherwise in a bad state, we have to abort and re-open the connection
	// to try again, as we can't realistically finish the write. We have to retry it, or return
	// and error to the user?

	// TODO configurable message retries

	// If there was not an error, and we simply didn't finish the write, we should enter
	// a write-until-complete loop, where we continue to write the data until the server accepts
	// all of it.

	// If both issues occurred, we'll need to find a way to determine if the error
	// is recoverable (is the connection in a bad state) or not

	var writeError error
	var totalBytesWritten = 0
	var bytesWritten = 0
	for totalBytesWritten < toWriteLen && writeError == nil {
		// While we haven't written enough yet
		// If there are remainder bytes, adjust the slice size we go to write
		// totalBytesWritten will be the index of the next Byte waiting to be read
		bytesWritten, writeError = u.socket.Write(rawBytes[totalBytesWritten:])
		totalBytesWritten += bytesWritten
	}

	if writeError != nil {
		if u.enableLogging {
			log.Printf("Error while writing data to %s. Expected to write %d, actually wrote %d. Underlying error: %s", u.address, toWriteLen, totalBytesWritten, writeError)
		}
		writeError = u.Close()
		if writeError != nil {
			// TODO ponder the following:
			// What if some bytes written, then failure, then also the close throws an error
			// []error is a better return type, but not sure if thats a thing you're supposed to do...
			// Possibilities for error not as complicated as i'm thinking?
			if u.enableLogging {
				// The error will get returned up the stack, no need to log it here?
				log.Printf("There was a subsequent error cleaning up the connection to %s", u.address)
			}
			return totalBytesWritten, writeError
		}
	}

	// Return the bytes written, any error
	return totalBytesWritten, writeError
}
