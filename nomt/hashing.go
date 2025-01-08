package nomt

import (
	"hash"

	"golang.org/x/crypto/sha3"
)

func commonPrefixBitLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}

	// First, find the first byte that differs.
	i := 0
	for i < n {
		if a[i] != b[i] {
			break
		}
		i++
	}
	if i == n {
		return n * 8
	}

	// Then, find the first bit that differs.
	diff := a[i] ^ b[i]
	j := 0
	for diff&0x80 == 0 {
		diff <<= 1
		j++
	}
	return i*8 + j
}

// Hash returns the root node of a Merkle tree assuming
// keys were updated. keys must be sorted lexicographically.
func (t *Tree) Hash(keys [][]byte) Node {
	h := sha3.NewLegacyKeccak256()

	for i := 0; i < len(keys); i++ {
		hashFrom := 0 // root node
		if i+1 < len(keys) {
			// if there is any common prefix, from the nodes from
			// the root up to the common prefix will get updated
			// with the next key.
			hashFrom = commonPrefixBitLen(keys[i], keys[i+1])
		}
		t.hash(h, keys[i], hashFrom)
	}
	return t.Root
}

func (t *Tree) hash(h hash.Hash, key []byte, hashFrom int) {
	var paddedKeyBuf [MaxKeyLenPadded]byte
	paddedKey := paddedKeyBuf[:]
	paddedKey, partialBits := PadKey(key, paddedKey)
	pageIdx := 0

	// Find the first node that needs to be updated.
	page := t.Pages[""] // start at the root
	for pageIdx < len(paddedKey)-1 {
		// If this node is not set, the continuation page does not exist.
		node := page.Nodes[indexOf(paddedKey[pageIdx], fullBits)]
		if node.IsZero() || node.IsLeaf() {
			break
		}
		pageIdx++
		page = t.Pages[string(paddedKey[:pageIdx])]
	}

	bits := byte(fullBits)
	if pageIdx == len(paddedKey)-1 {
		bits = byte(fullBits - partialBits)
	}
	pathLen := page.nonZeroPathBitLen(paddedKey[pageIdx], bits)
	// pathLen == 0 is not valid (key must be in the tree).

	nodeIdx := indexOf(paddedKey[pageIdx], pathLen)
	node := &page.Nodes[nodeIdx]

	for {
		// Need to find the node's sibling.
		// If the node's index is even, the sibling is +1.
		// If the node's index is odd, the sibling is -1.
		siblingIdx := nodeIdx + 1
		if nodeIdx&1 == 1 {
			siblingIdx = nodeIdx - 1
		}
		sibling := &page.Nodes[siblingIdx]

		node0, node1 := node, sibling
		if nodeIdx&1 == 1 {
			node0, node1 = sibling, node
		}

		// Walk back to find the parent.
		pathLen--

		// If we reached hashFrom, we are done.
		if hashFrom > 0 && 6*pageIdx+int(pathLen) <= hashFrom {
			break
		}

		if pathLen == 0 && pageIdx > 0 {
			// Need to walk back one page.
			pageIdx--
			page = t.Pages[string(paddedKey[:pageIdx])]
			pathLen = fullBits
		}
		parent := &t.Root
		parentIdx := byte(0)
		atRoot := pageIdx == 0 && pathLen == 0
		if !atRoot {
			parentIdx = indexOf(paddedKey[pageIdx], pathLen)
			parent = &page.Nodes[parentIdx]
		}
		node0.HashInto(h, t.Datastore)
		node1.HashInto(h, t.Datastore)
		hash := h.Sum(nil)
		h.Reset()
		copy(parent[:], hash)
		parent.MarkInternal()
		t.NumHashes++

		if atRoot {
			break
		}
		node = parent
		nodeIdx = parentIdx
	}
}
