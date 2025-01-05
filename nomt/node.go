package nomt

// Page is a 4KB block of data
// Contains 128-2=126 merkle tree nodes of 32 bytes each
// Root must be stored separately (or in the parent page)
// Last 64 bytes are reserved for metadata.
type Page struct {
	Nodes [126][32]byte
	_     [64]byte
}

// PadKey adds padding to the key such that:
// - Each 6 bits of the key are stored in the lower 6 bits of a byte.
// - The upper 2 bits of the byte are set to 0.
func PadKey(key []byte) ([]byte, bool) {
	out := make([]byte, (len(key)*8+5)/6) // ceil(len(key)*8/6)
	idx := 0
	for i, k := range key {
		switch i % 3 {
		case 0:
			out[idx] = k >> 2
			idx++
			out[idx] = (k & 0x03) << 4
		case 1:
			out[idx] |= k >> 4
			idx++
			out[idx] = (k & 0x0f) << 2
		case 2:
			out[idx] |= k >> 6
			idx++
			out[idx] = k & 0x3f
			idx++
		}
	}
	hasPartial := len(key)%3 != 0
	return out, hasPartial
}
