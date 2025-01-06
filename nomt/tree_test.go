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

	leafSize := unsafe.Sizeof(LeafNode{})
	t.Logf("Leaf node size: %d bytes", leafSize)
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
	require.Equal(t, 4, partial)

	t.Logf("Original key: %s", BytesToBinaryString(key))
	require.Equal(t, "0110100001100101011011000110110001101111", BytesToBinaryString(key))
	t.Logf("Padded key: %s", BytesToBinaryString(padded))
	require.Equal(t, "00011010000001100001010100101100000110110000011000111100", BytesToBinaryString(padded))
}

func TestPutGet(t *testing.T) {
	tr := NewTree()

	keys := []string{
		"001", "010", "011", "100", "101", "110", "111",
	}
	values := []string{
		"foo01", "foo02", "foo03", "foo04", "foo05", "foo06", "foo07",
	}

	for i, k := range keys {
		key := []byte(k)
		value := []byte(values[i])
		tr.Put(key, value)

		valBuf := make([]byte, 256)
		val, ok := tr.Get(key, valBuf)
		require.True(t, ok, "key %s", k)

		require.Equal(t, value, val)
	}
}
