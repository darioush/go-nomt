package nomt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkIndexAsInt(t *testing.T) {
	chunkIdx := ChunkIndex([3]byte{0, 1, 0})
	require.Equal(t, int(0x0100), chunkIdx.AsInt())
}

func TestLeafNodeChunks(t *testing.T) {
	d := New()
	leafNode := LeafNode{}

	k := []byte("hello")
	v := []byte("world")
	leafNode.PutKeyValue(k, v, d)

	var keyBuf [MaxKeyLen]byte
	key := keyBuf[:]
	key = leafNode.GetKey(key, d)
	require.Equal(t, k, key)

	var valueBuf [256]byte
	val := valueBuf[:]
	val = leafNode.GetValue(val, d)
	require.Equal(t, v, val)

	v2 := []byte("world2")
	leafNode.PutKeyValue(k, v2, d)
	val = leafNode.GetValue(val, d)
	require.Equal(t, v2, val)
}
