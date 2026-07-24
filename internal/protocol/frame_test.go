package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// ---------------------------------------------------------------------------
// RequestFrame round-trip
// ---------------------------------------------------------------------------

func TestRequestFrame_roundTrip(t *testing.T) {
	want := &RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 42,
		ClientID:      "test-client",
		Payload:       []byte("request-payload"),
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}

	got, err := ReadRequestFrame(&buf)
	if err != nil {
		t.Fatalf("ReadRequestFrame error: %v", err)
	}

	if got.ApiKey != want.ApiKey {
		t.Fatalf("ApiKey = %d, want %d", got.ApiKey, want.ApiKey)
	}
	if got.ApiVersion != want.ApiVersion {
		t.Fatalf("ApiVersion = %d, want %d", got.ApiVersion, want.ApiVersion)
	}
	if got.CorrelationID != want.CorrelationID {
		t.Fatalf("CorrelationID = %d, want %d", got.CorrelationID, want.CorrelationID)
	}
	if got.ClientID != want.ClientID {
		t.Fatalf("ClientID = %q, want %q", got.ClientID, want.ClientID)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Payload = %x, want %x", got.Payload, want.Payload)
	}
	if got.Size != int32(len("request-payload")+2+2+4+2+len("test-client")+4) {
		t.Fatalf("Size = %d, want encoded body length", got.Size)
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer has %d leftover bytes after decode", buf.Len())
	}
}

func TestRequestFrame_emptyPayload(t *testing.T) {
	want := &RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 7,
		ClientID:      "client",
		Payload:       []byte{},
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}

	got, err := ReadRequestFrame(&buf)
	if err != nil {
		t.Fatalf("ReadRequestFrame error: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("Payload length = %d, want 0", len(got.Payload))
	}
}

func TestRequestFrame_emptyClientID(t *testing.T) {
	want := &RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 99,
		ClientID:      "",
		Payload:       []byte("data"),
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}

	got, err := ReadRequestFrame(&buf)
	if err != nil {
		t.Fatalf("ReadRequestFrame error: %v", err)
	}
	if got.ClientID != "" {
		t.Fatalf("ClientID = %q, want empty string", got.ClientID)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Payload = %x, want %x", got.Payload, want.Payload)
	}
}

func TestRequestFrame_nilPayloadEncodesAsEmpty(t *testing.T) {
	f := &RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 5,
		ClientID:      "x",
		Payload:       nil,
	}

	var buf bytes.Buffer
	if _, err := f.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}

	got, err := ReadRequestFrame(&buf)
	if err != nil {
		t.Fatalf("ReadRequestFrame error: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("Payload length = %d, want 0", len(got.Payload))
	}
}

func TestRequestFrame_differentApiKeys(t *testing.T) {
	cases := []int16{0, 1, 18, 100}
	for _, apiKey := range cases {
		want := &RequestFrame{
			ApiKey:        apiKey,
			ApiVersion:    1,
			CorrelationID: 123,
			ClientID:      "api-test",
			Payload:       []byte{0x01, 0x02},
		}

		var buf bytes.Buffer
		if _, err := want.Write(&buf); err != nil {
			t.Fatalf("apiKey=%d Write error: %v", apiKey, err)
		}

		got, err := ReadRequestFrame(&buf)
		if err != nil {
			t.Fatalf("apiKey=%d ReadRequestFrame error: %v", apiKey, err)
		}
		if got.ApiKey != apiKey {
			t.Fatalf("apiKey=%d ApiKey = %d", apiKey, got.ApiKey)
		}
	}
}

func TestRequestFrame_binaryPayload(t *testing.T) {
	want := &RequestFrame{
		ApiKey:        1,
		ApiVersion:    1,
		CorrelationID: 55,
		ClientID:      "binary",
		Payload:       []byte{0x00, 0xff, 0x00, 0x00, 0xab, 0xcd},
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}

	got, err := ReadRequestFrame(&buf)
	if err != nil {
		t.Fatalf("ReadRequestFrame error: %v", err)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Payload = %x, want %x", got.Payload, want.Payload)
	}
}

func TestRequestFrame_writeUpdatesSizeField(t *testing.T) {
	f := &RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 42,
		ClientID:      "test-client",
		Payload:       []byte("request-payload"),
		Size:          -1,
	}

	var buf bytes.Buffer
	if _, err := f.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}

	expectedSize := int32(2 + 2 + 4 + 2 + len("test-client") + 4 + len("request-payload"))
	if f.Size != expectedSize {
		t.Fatalf("Size = %d, want %d", f.Size, expectedSize)
	}
}

// ---------------------------------------------------------------------------
// ResponseFrame round-trip
// ---------------------------------------------------------------------------

func TestResponseFrame_roundTrip(t *testing.T) {
	want := &ResponseFrame{
		CorrelationID: 42,
		ErrorCode:     0,
		Payload:       []byte("response-payload"),
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("ResponseFrame.Write error: %v", err)
	}

	got, err := ReadResponseFrame(&buf)
	if err != nil {
		t.Fatalf("ReadResponseFrame error: %v", err)
	}

	if got.CorrelationID != want.CorrelationID {
		t.Fatalf("CorrelationID = %d, want %d", got.CorrelationID, want.CorrelationID)
	}
	if got.ErrorCode != want.ErrorCode {
		t.Fatalf("ErrorCode = %d, want %d", got.ErrorCode, want.ErrorCode)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Payload = %x, want %x", got.Payload, want.Payload)
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer has %d leftover bytes after decode", buf.Len())
	}
}

func TestResponseFrame_errorCode(t *testing.T) {
	want := &ResponseFrame{
		CorrelationID: 42,
		ErrorCode:     3,
		Payload:       []byte("corrupt"),
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("ResponseFrame.Write error: %v", err)
	}

	got, err := ReadResponseFrame(&buf)
	if err != nil {
		t.Fatalf("ReadResponseFrame error: %v", err)
	}
	if got.ErrorCode != 3 {
		t.Fatalf("ErrorCode = %d, want 3", got.ErrorCode)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Payload = %x, want %x", got.Payload, want.Payload)
	}
}

func TestResponseFrame_emptyPayload(t *testing.T) {
	want := &ResponseFrame{
		CorrelationID: 8,
		ErrorCode:     0,
		Payload:       []byte{},
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("ResponseFrame.Write error: %v", err)
	}

	got, err := ReadResponseFrame(&buf)
	if err != nil {
		t.Fatalf("ReadResponseFrame error: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("Payload length = %d, want 0", len(got.Payload))
	}
}

func TestResponseFrame_binaryPayload(t *testing.T) {
	want := &ResponseFrame{
		CorrelationID: 77,
		ErrorCode:     0,
		Payload:       []byte{0x00, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef},
	}

	var buf bytes.Buffer
	if _, err := want.Write(&buf); err != nil {
		t.Fatalf("ResponseFrame.Write error: %v", err)
	}

	got, err := ReadResponseFrame(&buf)
	if err != nil {
		t.Fatalf("ReadResponseFrame error: %v", err)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("Payload = %x, want %x", got.Payload, want.Payload)
	}
}

func TestResponseFrame_writeUpdatesSizeField(t *testing.T) {
	f := &ResponseFrame{
		CorrelationID: 42,
		ErrorCode:     0,
		Payload:       []byte("response-payload"),
		Size:          -1,
	}

	var buf bytes.Buffer
	if _, err := f.Write(&buf); err != nil {
		t.Fatalf("ResponseFrame.Write error: %v", err)
	}

	expectedSize := int32(4 + 2 + 4 + len("response-payload"))
	if f.Size != expectedSize {
		t.Fatalf("Size = %d, want %d", f.Size, expectedSize)
	}
}

// ---------------------------------------------------------------------------
// Frame size limits
// ---------------------------------------------------------------------------

func TestRequestFrame_rejectsMaxFrameSizeExceeded(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(MaxFrameSize+1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}

	_, err := ReadRequestFrame(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadRequestFrame error = %v, want ErrFrameTooLarge", err)
	}
}

func TestResponseFrame_rejectsMaxFrameSizeExceeded(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(MaxFrameSize+1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}

	_, err := ReadResponseFrame(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadResponseFrame error = %v, want ErrFrameTooLarge", err)
	}
}

func TestRequestFrame_rejectsNegativeSize(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(-1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}

	_, err := ReadRequestFrame(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadRequestFrame error = %v, want ErrFrameTooLarge", err)
	}
}

func TestResponseFrame_rejectsNegativeSize(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(-1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}

	_, err := ReadResponseFrame(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("ReadResponseFrame error = %v, want ErrFrameTooLarge", err)
	}
}

func TestRequestFrame_acceptsMaxFrameSize(t *testing.T) {
	// ApiKey(2) + ApiVersion(2) + CorrelationID(4) + ClientID length(2) + 0 + Payload length(4)
	payloadLen := MaxFrameSize - (2 + 2 + 4 + 2 + 4)
	payload := make([]byte, payloadLen)

	f := &RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 1,
		ClientID:      "",
		Payload:       payload,
	}

	var buf bytes.Buffer
	if _, err := f.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}
	if f.Size != MaxFrameSize {
		t.Fatalf("Size = %d, want MaxFrameSize", f.Size)
	}

	got, err := ReadRequestFrame(&buf)
	if err != nil {
		t.Fatalf("ReadRequestFrame error: %v", err)
	}
	if int(got.Size) != MaxFrameSize {
		t.Fatalf("decoded Size = %d, want MaxFrameSize", got.Size)
	}
	if len(got.Payload) != len(payload) {
		t.Fatalf("Payload length = %d, want %d", len(got.Payload), len(payload))
	}
}

// ---------------------------------------------------------------------------
// Size mismatch
// ---------------------------------------------------------------------------

// Body is shorter than declared size and there is trailing data after the
// frame body. The limited reader still has budget and reads a non-zero byte,
// so ErrFrameSizeMismatch is returned.
func TestRequestFrame_bodyShorterThanDeclaredWithTrailingData(t *testing.T) {
	body := []byte{
		0x00, 0x00, // ApiKey
		0x00, 0x01, // ApiVersion
		0x00, 0x00, 0x00, 0x2a, // CorrelationID
		0x00, 0x00, // ClientID length = 0
		0x00, 0x00, 0x00, 0x00, // Payload length = 0
	}

	var buf bytes.Buffer
	// Declare size larger than the actual body.
	if err := binary.Write(&buf, binary.BigEndian, int32(len(body)+1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write(body)
	buf.Write([]byte{0xff}) // trailing byte the LimitedReader can still consume

	_, err := ReadRequestFrame(&buf)
	if !errors.Is(err, ErrFrameSizeMismatch) {
		t.Fatalf("ReadRequestFrame error = %v, want ErrFrameSizeMismatch", err)
	}
}

func TestResponseFrame_bodyShorterThanDeclaredWithTrailingData(t *testing.T) {
	body := []byte{
		0x00, 0x00, 0x00, 0x2a, // CorrelationID
		0x00, 0x00, // ErrorCode
		0x00, 0x00, 0x00, 0x00, // Payload length = 0
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(len(body)+1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write(body)
	buf.Write([]byte{0xff})

	_, err := ReadResponseFrame(&buf)
	if !errors.Is(err, ErrFrameSizeMismatch) {
		t.Fatalf("ReadResponseFrame error = %v, want ErrFrameSizeMismatch", err)
	}
}

// Body is shorter than declared size and the reader is empty afterwards. The
// probe read returns EOF; the decoder still reports an error, but it is an
// I/O error rather than ErrFrameSizeMismatch.
func TestRequestFrame_bodyShorterThanDeclaredNoTrailingData(t *testing.T) {
	body := []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x2a} // ApiKey(2) + ApiVersion(2) + CorrelationID(4)

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(100)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write(body)

	_, err := ReadRequestFrame(&buf)
	if err == nil {
		t.Fatal("ReadRequestFrame expected error for short body, got nil")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadRequestFrame error = %v, want io.EOF or io.ErrUnexpectedEOF", err)
	}
}

func TestResponseFrame_bodyShorterThanDeclaredNoTrailingData(t *testing.T) {
	body := []byte{0x00, 0x00, 0x00, 0x2a} // CorrelationID(4)

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(100)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write(body)

	_, err := ReadResponseFrame(&buf)
	if err == nil {
		t.Fatal("ReadResponseFrame expected error for short body, got nil")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadResponseFrame error = %v, want io.EOF or io.ErrUnexpectedEOF", err)
	}
}

// Declared size is smaller than the actual body. The decoder runs out of
// LimitedReader budget while reading a field and returns io.ErrUnexpectedEOF.
func TestRequestFrame_bodyLongerThanDeclared(t *testing.T) {
	want := &RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 42,
		ClientID:      "x",
		Payload:       []byte("payload"),
	}

	var body bytes.Buffer
	if _, err := PutInt16(&body, want.ApiKey); err != nil {
		t.Fatalf("PutInt16 error: %v", err)
	}
	if _, err := PutInt16(&body, want.ApiVersion); err != nil {
		t.Fatalf("PutInt16 error: %v", err)
	}
	if _, err := PutInt32(&body, want.CorrelationID); err != nil {
		t.Fatalf("PutInt32 error: %v", err)
	}
	if _, err := PutString(&body, want.ClientID); err != nil {
		t.Fatalf("PutString error: %v", err)
	}
	if _, err := PutBytes(&body, want.Payload); err != nil {
		t.Fatalf("PutBytes error: %v", err)
	}

	var buf bytes.Buffer
	// Declare size smaller than the actual body.
	if err := binary.Write(&buf, binary.BigEndian, int32(body.Len()-1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write(body.Bytes())

	_, err := ReadRequestFrame(&buf)
	if err == nil {
		t.Fatal("ReadRequestFrame expected error, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadRequestFrame error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestResponseFrame_bodyLongerThanDeclared(t *testing.T) {
	want := &ResponseFrame{
		CorrelationID: 42,
		ErrorCode:     0,
		Payload:       []byte("payload"),
	}

	var body bytes.Buffer
	if _, err := PutInt32(&body, want.CorrelationID); err != nil {
		t.Fatalf("PutInt32 error: %v", err)
	}
	if _, err := PutInt16(&body, want.ErrorCode); err != nil {
		t.Fatalf("PutInt16 error: %v", err)
	}
	if _, err := PutBytes(&body, want.Payload); err != nil {
		t.Fatalf("PutBytes error: %v", err)
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(body.Len()-1)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write(body.Bytes())

	_, err := ReadResponseFrame(&buf)
	if err == nil {
		t.Fatal("ReadResponseFrame expected error, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadResponseFrame error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// ---------------------------------------------------------------------------
// Truncated input
// ---------------------------------------------------------------------------

func TestRequestFrame_truncatedSizeField(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00, 0x00, 0x00}) // only 3 of 4 bytes
	_, err := ReadRequestFrame(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadRequestFrame error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestResponseFrame_truncatedSizeField(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00, 0x00, 0x00}) // only 3 of 4 bytes
	_, err := ReadResponseFrame(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadResponseFrame error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestRequestFrame_truncatedBody(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(20)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write([]byte{0x00, 0x00, 0x00, 0x01}) // only 4 of 20 bytes

	_, err := ReadRequestFrame(&buf)
	if err == nil {
		t.Fatal("ReadRequestFrame expected error, got nil")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadRequestFrame error = %v, want io.EOF or io.ErrUnexpectedEOF", err)
	}
}

func TestResponseFrame_truncatedBody(t *testing.T) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, int32(20)); err != nil {
		t.Fatalf("binary.Write error: %v", err)
	}
	buf.Write([]byte{0x00, 0x00, 0x00, 0x2a}) // only 4 of 20 bytes

	_, err := ReadResponseFrame(&buf)
	if err == nil {
		t.Fatal("ReadResponseFrame expected error, got nil")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadResponseFrame error = %v, want io.EOF or io.ErrUnexpectedEOF", err)
	}
}

// ---------------------------------------------------------------------------
// Multiple frames
// ---------------------------------------------------------------------------

func TestRequestFrame_multipleSequentialFrames(t *testing.T) {
	frames := []*RequestFrame{
		{ApiKey: 0, ApiVersion: 1, CorrelationID: 1, ClientID: "a", Payload: []byte("p1")},
		{ApiKey: 1, ApiVersion: 1, CorrelationID: 2, ClientID: "b", Payload: []byte("p2")},
	}

	var buf bytes.Buffer
	for _, f := range frames {
		if _, err := f.Write(&buf); err != nil {
			t.Fatalf("RequestFrame.Write error: %v", err)
		}
	}

	for i, want := range frames {
		got, err := ReadRequestFrame(&buf)
		if err != nil {
			t.Fatalf("frame %d ReadRequestFrame error: %v", i, err)
		}
		if got.CorrelationID != want.CorrelationID {
			t.Fatalf("frame %d CorrelationID = %d, want %d", i, got.CorrelationID, want.CorrelationID)
		}
		if got.ApiKey != want.ApiKey {
			t.Fatalf("frame %d ApiKey = %d, want %d", i, got.ApiKey, want.ApiKey)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("frame %d Payload = %x, want %x", i, got.Payload, want.Payload)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer has %d leftover bytes after decoding all frames", buf.Len())
	}
}

func TestResponseFrame_multipleSequentialFrames(t *testing.T) {
	frames := []*ResponseFrame{
		{CorrelationID: 1, ErrorCode: 0, Payload: []byte("r1")},
		{CorrelationID: 2, ErrorCode: 3, Payload: []byte("r2")},
	}

	var buf bytes.Buffer
	for _, f := range frames {
		if _, err := f.Write(&buf); err != nil {
			t.Fatalf("ResponseFrame.Write error: %v", err)
		}
	}

	for i, want := range frames {
		got, err := ReadResponseFrame(&buf)
		if err != nil {
			t.Fatalf("frame %d ReadResponseFrame error: %v", i, err)
		}
		if got.CorrelationID != want.CorrelationID {
			t.Fatalf("frame %d CorrelationID = %d, want %d", i, got.CorrelationID, want.CorrelationID)
		}
		if got.ErrorCode != want.ErrorCode {
			t.Fatalf("frame %d ErrorCode = %d, want %d", i, got.ErrorCode, want.ErrorCode)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("frame %d Payload = %x, want %x", i, got.Payload, want.Payload)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer has %d leftover bytes after decoding all frames", buf.Len())
	}
}

func TestMixedFrames_multipleSequentialFrames(t *testing.T) {
	req := &RequestFrame{ApiKey: 1, ApiVersion: 1, CorrelationID: 7, ClientID: "mixed", Payload: []byte("req")}
	resp := &ResponseFrame{CorrelationID: 7, ErrorCode: 0, Payload: []byte("resp")}

	var buf bytes.Buffer
	if _, err := req.Write(&buf); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}
	if _, err := resp.Write(&buf); err != nil {
		t.Fatalf("ResponseFrame.Write error: %v", err)
	}

	gotReq, err := ReadRequestFrame(&buf)
	if err != nil {
		t.Fatalf("ReadRequestFrame error: %v", err)
	}
	if gotReq.CorrelationID != req.CorrelationID {
		t.Fatalf("Request CorrelationID = %d, want %d", gotReq.CorrelationID, req.CorrelationID)
	}

	gotResp, err := ReadResponseFrame(&buf)
	if err != nil {
		t.Fatalf("ReadResponseFrame error: %v", err)
	}
	if gotResp.CorrelationID != resp.CorrelationID {
		t.Fatalf("Response CorrelationID = %d, want %d", gotResp.CorrelationID, resp.CorrelationID)
	}
	if buf.Len() != 0 {
		t.Fatalf("buffer has %d leftover bytes", buf.Len())
	}
}
