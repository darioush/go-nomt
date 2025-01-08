package nomt

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
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
	var paddedBuf [MaxKeyLenPadded]byte
	key := []byte("hello")
	padded := paddedBuf[:]
	padded, partial := PadKey(key, padded)
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

	lastHashes := uint64(0)
	for i, k := range keys {
		key := []byte(k)
		value := []byte(values[i])
		tr.Put(key, value)

		valBuf := make([]byte, 256)
		val, ok := tr.Get(key, valBuf)
		require.True(t, ok, "key %s", k)

		require.Equal(t, value, val)

		hash := tr.Hash([][]byte{key})
		t.Logf("Hashes: %d Root: %x", tr.NumHashes-uint64(lastHashes), hash)
		lastHashes = tr.NumHashes
	}
}

func TestPutGetRandom(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	hasher := sha3.NewLegacyKeccak256()

	getKey := func(keyIdx int) []byte {
		k := fmt.Sprintf("key-%d", keyIdx)
		hasher.Write([]byte(k))
		hash := hasher.Sum(nil)
		hasher.Reset()
		return hash
	}

	nextKeyIdx := 0
	nextKey := func() []byte {
		key := getKey(nextKeyIdx)
		nextKeyIdx++
		return key
	}

	randomVal := func() []byte {
		v := make([]byte, r.Intn(256))
		r.Read(v)
		return v
	}

	var valBuf [256]byte

	tr := NewTree()
	mapStore := make(map[string]string)
	checkEach := 100
	verbose := false
	for i := 0; i < 10_000; i++ {
		if i%2 == 0 {
			key, value := nextKey(), randomVal()
			if verbose {
				t.Logf("Op: Put %x -> %x", key, value)
			}
			gotVal := valBuf[:]
			_, ok := tr.Get(key, gotVal)
			require.False(t, ok)

			tr.Put(key, value)
			gotVal = valBuf[:]
			gotVal, ok = tr.Get(key, gotVal)
			require.True(t, ok)
			require.Equal(t, value, gotVal)
			mapStore[string(key)] = string(value)
		} else {
			keyIdx := rand.Intn(len(mapStore))
			key := getKey(keyIdx)
			gotVal := valBuf[:]
			gotVal, ok := tr.Get([]byte(key), gotVal)
			require.True(t, ok)
			require.Equal(t, []byte(mapStore[string(key)]), gotVal)
			value := randomVal()
			if verbose {
				t.Logf("Op: Update %x -> %x", key, value)
			}
			tr.Put([]byte(key), value)

			gotVal, ok = tr.Get([]byte(key), gotVal)
			require.True(t, ok)
			require.Equal(t, value, gotVal)
			mapStore[string(key)] = string(value)
		}

		if (i+1)%checkEach == 0 {
			if verbose {
				t.Logf("Op: Check all (%d ops)", i+1)
			}
			for key, value := range mapStore {
				gotVal := valBuf[:]
				gotVal, ok := tr.Get([]byte(key), gotVal)
				require.True(t, ok)
				require.Equal(t, []byte(value), gotVal)
			}
		}
	}
}

func BenchmarkPut(b *testing.B) {
	hasher := sha3.NewLegacyKeccak256()

	getKey := func(keyIdx int, buf []byte) {
		pos := copy(buf, "key-")
		binary.BigEndian.PutUint64(buf[pos:], uint64(keyIdx))
		hasher.Write(buf[:pos+8])
		hash := hasher.Sum(nil)
		hasher.Reset()

		copy(buf, hash)
	}

	for _, initialSize := range []int{100, 1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("InitialSize-%d", initialSize), func(b *testing.B) {
			tr := NewTree()
			var keyBuf [32]byte
			var value [255]byte
			maxKey := 1 << 19
			for i := 0; i < initialSize; i++ {
				getKey(i%maxKey, keyBuf[:])
				binary.BigEndian.PutUint64(value[:], uint64(i))
				tr.Put(keyBuf[:], value[:])
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				getKey((initialSize+i)%maxKey, keyBuf[:])
				binary.BigEndian.PutUint64(value[:], uint64(i))
				tr.Put(keyBuf[:], value[:])
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	hasher := sha3.NewLegacyKeccak256()

	getKey := func(keyIdx int, buf []byte) {
		pos := copy(buf, "key-")
		binary.BigEndian.PutUint64(buf[pos:], uint64(keyIdx))
		hasher.Write(buf[:pos+8])
		hash := hasher.Sum(nil)
		hasher.Reset()

		copy(buf, hash)
	}

	for _, initialSize := range []int{100, 1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("InitialSize-%d", initialSize), func(b *testing.B) {
			tr := NewTree()
			var keyBuf [32]byte
			var value [255]byte
			for i := 0; i < initialSize; i++ {
				getKey(i, keyBuf[:])
				binary.BigEndian.PutUint64(value[:], uint64(i))
				tr.Put(keyBuf[:], value[:])
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				getKey(i%initialSize, keyBuf[:])
				gotVal, ok := tr.Get(keyBuf[:], value[:])
				require.True(b, ok)
				require.Equal(b, i%initialSize, int(binary.BigEndian.Uint64(gotVal)))
			}
		})
	}

}
