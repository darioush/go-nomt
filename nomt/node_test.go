package nomt

import (
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestPageSize(t *testing.T) {
	pageSize := unsafe.Sizeof(Page{})
	t.Logf("Page size: %d bytes", pageSize)
}

func BytesToBinaryString(data []byte) string {
	var sb strings.Builder
	for _, b := range data {
		sb.WriteString(fmt.Sprintf("%08b", b))
	}
	return sb.String()
}

func TestPadKey(t *testing.T) {
	key := []byte("hello")
	padded, partial := PadKey(key)
	require.True(t, partial)

	t.Logf("Original key: %s", BytesToBinaryString(key))
	require.Equal(t, "0110100001100101011011000110110001101111", BytesToBinaryString(key))
	t.Logf("Padded key: %s", BytesToBinaryString(padded))
	require.Equal(t, "00011010000001100001010100101100000110110000011000111100", BytesToBinaryString(padded))
}
