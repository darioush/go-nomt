package nomt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	MaxKeyLen       = 64
	MaxKeyLenPadded = (MaxKeyLen*8 + 5) / 6
)

var Zero Node

const fullBits = 6

// Page is a 4KB block of data
// Contains 128-2=126 merkle tree nodes of 32 bytes each
// Root must be stored separately (or in the parent page)
// Last 64 bytes are reserved for metadata.
type Page struct {
	Nodes [126]Node
	_     [64]byte
}

func (p *Page) print() {
	for i, node := range p.Nodes {
		fmt.Printf("%d: %x\n", i, node)
	}
}

func (p *Page) nonZeroPathBitLen(query byte, bitLen byte) byte {
	i := byte(0)
	for i < bitLen {
		node := &p.Nodes[indexOf(query, i+1)]
		if node.IsZero() {
			return i
		}
		i++
	}
	return i
}

// PadKey adds padding to the key such that:
// - Each 6 bits of the key are stored in the lower 6 bits of a byte.
// - The upper 2 bits of the byte are set to 0.
// Returns the padded key and the number of partial bits in the last byte.
func PadKey(key []byte) ([]byte, int) {
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
	return out, 2 * (len(key) % 3)
}

// indexOf returns the index of the query byte in the page array.
func indexOf(query byte, bitLen byte) byte {
	idx := 1<<bitLen | (query >> (fullBits - bitLen)) // TODO: see if we should change the partial byte format to avoid this shift
	return idx - 2
}

type Tree struct {
	Root      [32]byte
	Pages     map[string]*Page // TODO: consider indexing into an array of pages
	Datastore *Datastore
}

func NewTree() *Tree {
	return &Tree{
		Pages: map[string]*Page{
			"": {},
		},
		Datastore: New(),
	}
}

func (t *Tree) Get(key []byte, valBuf []byte) ([]byte, bool) {
	paddedKey, partialBits := PadKey(key)
	pageIdx := 0
	// The last byte in the padded key always indexes into the page.
	// This page may be the root page or a page with a path that is a prefix of the key.

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
	if pathLen == 0 {
		return nil, false
	}
	node := &page.Nodes[indexOf(paddedKey[pageIdx], pathLen)]
	if !node.IsLeaf() {
		return nil, false
	}

	var keyBuf [MaxKeyLen]byte
	foundKey := keyBuf[:]
	leaf := (*LeafNode)(unsafe.Pointer(node))
	foundKey = leaf.GetKey(foundKey, t.Datastore)
	if !bytes.Equal(foundKey, key) {
		return nil, false
	}
	return leaf.GetValue(valBuf, t.Datastore), true
}

func (t *Tree) Put(key, value []byte) {
	paddedKey, partialBits := PadKey(key)
	pageIdx := 0
	// The last byte in the padded key always indexes into the page.
	// This page may be the root page or a page with a path that is a prefix of the key.

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
	getOrAllocate := func(paddedKey []byte, pathLen byte) *Node {
		if pathLen == fullBits {
			// Need a new page
			page = &Page{}
			pageIdx++
			t.Pages[string(paddedKey[:pageIdx])] = page
			// Since this is a new page, 1 bits is used here.
			return &page.Nodes[indexOf(paddedKey[pageIdx], 1)]
		}
		return &page.Nodes[indexOf(paddedKey[pageIdx], pathLen+1)]
	}

	pathLen := page.nonZeroPathBitLen(paddedKey[pageIdx], bits)
	if pathLen == 0 {
		// Create a new leaf node at pathLen+1
		ptr := getOrAllocate(paddedKey, pathLen)
		leafNode := (*LeafNode)(unsafe.Pointer(ptr))
		leafNode.PutKeyValue(key, value, t.Datastore)
		return
	}

	node := &page.Nodes[indexOf(paddedKey[pageIdx], pathLen)]
	if !node.IsLeaf() {
		ptr := getOrAllocate(paddedKey, pathLen)
		leafNode := (*LeafNode)(unsafe.Pointer(ptr))
		leafNode.PutKeyValue(key, value, t.Datastore)
		return
	}

	var keyBuf [MaxKeyLen]byte
	foundKey := keyBuf[:]
	leaf := (*LeafNode)(unsafe.Pointer(node))
	foundKey = leaf.GetKey(foundKey, t.Datastore)
	if bytes.Equal(foundKey, key) {
		leaf.PutValue(value, t.Datastore)
		return
	}

	// Split the leaf node
	foundKeyPadded, _ := PadKey(foundKey)
	// Up until pageIdx:pathLen, the keys are guaranteed to be the same.
	// We need to find the first bit where the keys differ.
	var nextNode *Node
	for {
		nextNode = getOrAllocate(paddedKey, pathLen)
		if pathLen == fullBits {
			// new page was allocated
			pathLen = 0
		}
		if paddedKey[pageIdx]&(1<<(fullBits-pathLen-1)) != foundKeyPadded[pageIdx]&(1<<(fullBits-pathLen-1)) {
			break
		}
		nextNode.MarkDirty()
		pathLen++
	}
	// At pathLen, the keys differ.
	// Note keys MUST not be prefixes of each other.
	leafNode := (*LeafNode)(unsafe.Pointer(nextNode))
	leafNode.PutKeyValue(key, value, t.Datastore)

	copyNode := getOrAllocate(foundKeyPadded, pathLen)
	*copyNode = *node

	node.MarkDirty() // Mark the old leaf node as dirty (internal node)
}

func (t *Tree) print() {
	for path, page := range t.Pages {
		fmt.Printf("Path: %x\n", path)
		page.print()
	}
}
