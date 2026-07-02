package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(0)

	if len(os.Args) != 2 {
		failf("usage: native <mode>")
	}

	var err error
	switch os.Args[1] {
	case "client-half-close":
		err = runClientHalfClose()
	case "client-many-roundtrip":
		err = runClientManyRoundTrip()
	case "client-open-after-goaway":
		err = runClientOpenAfterGoAway()
	case "malformed-frame":
		err = runMalformedFrame()
	case "server-close-immediately":
		err = runServerCloseImmediately()
	case "server-echo":
		err = runServerEcho()
	case "server-expect-malformed":
		err = runServerExpectMalformed()
	case "server-expect-reset":
		err = runServerExpectReset()
	case "server-goaway":
		err = runServerGoAway()
	case "server-reset-after-close-timeout":
		err = runServerResetAfterCloseTimeout()
	case "server-reply-after-fin":
		err = runServerReplyAfterFin()
	case "client-roundtrip":
		err = runClientRoundTrip()
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		failf("%v", err)
	}
}

func runServerEcho() error {
	session, err := yamux.Server(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}
	defer session.Close()

	var wg sync.WaitGroup
	for {
		stream, err := session.Accept()
		if err != nil {
			break
		}

		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer conn.Close()
			if _, copyErr := io.Copy(conn, conn); copyErr != nil && !isClosedErr(copyErr) {
				log.Printf("echo stream failed: %v", copyErr)
			}
		}(stream)
	}

	wg.Wait()
	return nil
}

func runServerReplyAfterFin() error {
	payload, err := payloadFromEnv()
	if err != nil {
		return err
	}

	session, err := yamux.Server(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}

	stream, err := session.Accept()
	if err != nil {
		return err
	}
	defer stream.Close()

	received, err := io.ReadAll(stream)
	if err != nil {
		return err
	}
	if !bytes.Equal(received, payload) {
		return fmt.Errorf("payload mismatch before FIN: wrote %d bytes, read %d matching bytes", len(payload), commonPrefix(payload, received))
	}

	return writeFull(stream, payload)
}

func runServerExpectReset() error {
	session, err := yamux.Server(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}

	stream, err := session.Accept()
	if err != nil {
		return err
	}

	_, err = io.Copy(io.Discard, stream)
	if errors.Is(err, yamux.ErrConnectionReset) {
		return nil
	}
	return fmt.Errorf("expected connection reset, got %v", err)
}

func runServerGoAway() error {
	session, err := yamux.Server(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}

	if err := session.GoAway(); err != nil {
		return err
	}
	time.Sleep(holdDuration())
	return nil
}

func runServerResetAfterCloseTimeout() error {
	config := yamuxConfig()
	config.StreamCloseTimeout = 50 * time.Millisecond
	session, err := yamux.Server(newStdioConn(), config)
	if err != nil {
		return err
	}

	stream, err := session.Accept()
	if err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	time.Sleep(holdDuration())
	return nil
}

func runServerCloseImmediately() error {
	_, err := yamux.Server(newStdioConn(), yamuxConfig())
	return err
}

func runServerExpectMalformed() error {
	session, err := yamux.Server(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}

	_, err = session.Accept()
	if errors.Is(err, yamux.ErrInvalidVersion) || errors.Is(err, yamux.ErrInvalidMsgType) {
		return nil
	}
	return fmt.Errorf("expected malformed frame error, got %v", err)
}

func runClientRoundTrip() error {
	payload, err := payloadFromEnv()
	if err != nil {
		return err
	}

	session, err := yamux.Client(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}
	defer session.Close()

	stream, err := session.Open()
	if err != nil {
		return err
	}
	defer stream.Close()

	if err := writeFull(stream, payload); err != nil {
		return err
	}

	received := make([]byte, len(payload))
	if len(received) > 0 {
		if _, err := io.ReadFull(stream, received); err != nil {
			return err
		}
	}

	if !bytes.Equal(received, payload) {
		return fmt.Errorf("payload mismatch: wrote %d bytes, read %d matching bytes", len(payload), commonPrefix(payload, received))
	}

	return nil
}

func runClientHalfClose() error {
	payload, err := payloadFromEnv()
	if err != nil {
		return err
	}

	session, err := yamux.Client(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}
	defer session.Close()

	stream, err := session.Open()
	if err != nil {
		return err
	}
	defer stream.Close()

	if err := writeFull(stream, payload); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}

	received := make([]byte, len(payload))
	if len(received) > 0 {
		if _, err := io.ReadFull(stream, received); err != nil {
			return err
		}
	}
	if !bytes.Equal(received, payload) {
		return fmt.Errorf("payload mismatch after FIN: wrote %d bytes, read %d matching bytes", len(payload), commonPrefix(payload, received))
	}
	return nil
}

func runClientManyRoundTrip() error {
	count := integerFromEnv("YAMUX_STREAM_COUNT", 16)
	length := integerFromEnv("YAMUX_PAYLOAD_LENGTH", 1024)

	session, err := yamux.Client(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}
	defer session.Close()

	var wg sync.WaitGroup
	errs := make(chan error, count)
	for index := 0; index < count; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := deterministicPayload(length, index+100)
			stream, err := session.Open()
			if err != nil {
				errs <- err
				return
			}
			defer stream.Close()
			if err := writeFull(stream, payload); err != nil {
				errs <- err
				return
			}

			received := make([]byte, len(payload))
			if len(received) > 0 {
				if _, err := io.ReadFull(stream, received); err != nil {
					errs <- err
					return
				}
			}
			if !bytes.Equal(received, payload) {
				errs <- fmt.Errorf("stream %d mismatch: wrote %d bytes, read %d matching bytes", index, len(payload), commonPrefix(payload, received))
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func runClientOpenAfterGoAway() error {
	session, err := yamux.Client(newStdioConn(), yamuxConfig())
	if err != nil {
		return err
	}

	time.Sleep(holdDuration())
	stream, err := session.Open()
	if stream != nil {
		stream.Close()
	}
	if errors.Is(err, yamux.ErrRemoteGoAway) {
		return nil
	}
	return fmt.Errorf("expected remote go away, got %v", err)
}

func runMalformedFrame() error {
	_, err := os.Stdout.Write([]byte{
		1, 2, 0, 1,
		0, 0, 0, 0,
		0, 0, 0, 0,
	})
	return err
}

func yamuxConfig() *yamux.Config {
	config := yamux.DefaultConfig()
	config.EnableKeepAlive = false
	config.LogOutput = os.Stderr
	return config
}

func payloadFromEnv() ([]byte, error) {
	encoded := os.Getenv("YAMUX_PAYLOAD_BASE64")
	if encoded == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(encoded)
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := writer.Write(payload)
		payload = payload[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func deterministicPayload(length int, seed int) []byte {
	payload := make([]byte, length)
	for index := range payload {
		payload[index] = byte((index*31 + seed*17) & 0xff)
	}
	return payload
}

func integerFromEnv(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func holdDuration() time.Duration {
	return time.Duration(integerFromEnv("YAMUX_HOLD_MS", 100)) * time.Millisecond
}

func commonPrefix(left []byte, right []byte) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index] != right[index] {
			return index
		}
	}
	return limit
}

func isClosedErr(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, os.ErrClosed)
}

func failf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}

type stdioConn struct {
	reader  *os.File
	writer  *os.File
	writeMu sync.Mutex
	closeMu sync.Mutex
	closed  bool
}

func newStdioConn() net.Conn {
	return &stdioConn{
		reader: os.Stdin,
		writer: os.Stdout,
	}
}

func (conn *stdioConn) Read(payload []byte) (int, error) {
	return conn.reader.Read(payload)
}

func (conn *stdioConn) Write(payload []byte) (int, error) {
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()

	total := 0
	for total < len(payload) {
		written, err := conn.writer.Write(payload[total:])
		total += written
		if err != nil {
			return total, err
		}
		if written == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (conn *stdioConn) Close() error {
	conn.closeMu.Lock()
	defer conn.closeMu.Unlock()
	if conn.closed {
		return nil
	}
	conn.closed = true
	_ = conn.reader.Close()
	_ = conn.writer.Close()
	return nil
}

func (conn *stdioConn) LocalAddr() net.Addr {
	return stdioAddr("yamux-ts")
}

func (conn *stdioConn) RemoteAddr() net.Addr {
	return stdioAddr("hashicorp-yamux")
}

func (conn *stdioConn) SetDeadline(_ time.Time) error {
	return nil
}

func (conn *stdioConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (conn *stdioConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

type stdioAddr string

func (addr stdioAddr) Network() string {
	return "stdio"
}

func (addr stdioAddr) String() string {
	return string(addr)
}
