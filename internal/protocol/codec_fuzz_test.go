package protocol

import (
	"bytes"
	"testing"
)

// FuzzCodec ensures that decoding arbitrary byte streams into protocol types
// never panics or triggers out-of-bounds index errors.
func FuzzCodec(f *testing.F) {
	// Seed initial corpus with valid/invalid byte sequences
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 1, 0, 0, 0, 1, 'a', 0, 0, 0, 0})
	f.Add([]byte{255, 255, 255, 255})

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)

		// Test primitive decoders
		_, _ = Int8(r)

		r.Reset(data)
		_, _ = Int16(r)

		r.Reset(data)
		_, _ = Int32(r)

		r.Reset(data)
		_, _ = Int64(r)

		r.Reset(data)
		_, _ = String(r)

		r.Reset(data)
		_, _ = Bytes(r)

		r.Reset(data)
		var produceReq ProduceRequest
		_ = produceReq.Decode(r)

		r.Reset(data)
		var fetchReq FetchRequest
		_ = fetchReq.Decode(r)

		r.Reset(data)
		var apiVersionsResp ApiVersionsResponse
		_ = apiVersionsResp.Decode(r)
	})
}
