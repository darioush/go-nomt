package nomt

import (
	"bytes"
)

type (
	Node       [32]byte
	ChunkIndex [4]byte
)

func (c *ChunkIndex) AsInt() uint32 {
	return uint32(c[0])<<24 | uint32(c[1])<<16 | uint32(c[2])<<8 | uint32(c[3])
}

func (n *Node) IsLeaf() bool {
	// Leaf nodes have most significant bit set to 0.
	return n[0]>>7 == 0
}

func (n *Node) IsZero() bool {
	return bytes.Equal(n[:], Zero[:])
}

func (n *Node) MarkDirty() {
	// Mark the node as dirty by setting the most significant bit to 1.
	// This means it cannot be a leaf node.
	n[0] |= 0x80
}

type LeafNode struct {
	_        byte // ignored
	KeyLen   byte
	ValueLen byte
	Chunks   [7]ChunkIndex
	_        [1]byte // ignored
}

func (l *LeafNode) get(buf []byte, startChunk, chunkPos int, length int, db *Datastore) {
	pos, chunk := 0, startChunk
	for pos < length {
		last := pos + (ChunkSize - chunkPos)
		if last > length {
			last = length
		}
		chunkID := l.Chunks[chunk].AsInt()
		pos = pos + copy(buf[pos:last], db.Data[chunkID][chunkPos:])
		chunk++
		chunkPos = 0 // reading next chunk always starts at the beginning
	}
}

func (l *LeafNode) put(buf []byte, startChunk, chunkPos int, length int, db *Datastore) {
	pos, chunk := 0, startChunk
	for pos < length {
		last := pos + (ChunkSize - chunkPos)
		if last > length {
			last = length
		}
		chunkID := l.Chunks[chunk].AsInt()
		pos = pos + copy(db.Data[chunkID][chunkPos:], buf[pos:last])
		chunk++
		chunkPos = 0 // writing next chunk always starts at the beginning
	}
}

func (l *LeafNode) valueStart() (int, int) {
	chunk := int(l.KeyLen) / ChunkSize
	chunkPos := int(l.KeyLen) % ChunkSize
	return int(chunk), chunkPos
}

func (l *LeafNode) GetKey(buf []byte, db *Datastore) []byte {
	l.get(buf, 0, 0, int(l.KeyLen), db)
	return buf[:l.KeyLen]
}

func (l *LeafNode) GetValue(buf []byte, db *Datastore) []byte {
	chunk, chunkPos := l.valueStart()
	l.get(buf, chunk, chunkPos, int(l.ValueLen), db)
	return buf[:l.ValueLen]
}

func (l *LeafNode) PutValue(value []byte, db *Datastore) {
	l.allocExact(
		numChunks(int(l.KeyLen), int(l.ValueLen)),
		numChunks(int(l.KeyLen), len(value)),
		db,
	)
	l.ValueLen = byte(len(value))
	chunk, chunkPos := l.valueStart()
	l.put(value, chunk, chunkPos, len(value), db)
}

func (l *LeafNode) PutKeyValue(key, value []byte, db *Datastore) {
	l.allocExact(
		numChunks(int(l.KeyLen), int(l.ValueLen)),
		numChunks(len(key), len(value)),
		db,
	)
	l.KeyLen = byte(len(key))
	l.ValueLen = byte(len(value))

	l.put(key, 0, 0, len(key), db)
	chunk, chunkPos := l.valueStart()
	l.put(value, chunk, chunkPos, len(value), db)
}

func numChunks(keyLen, valueLen int) int {
	return (keyLen + valueLen + ChunkSize - 1) / ChunkSize
}

func (l *LeafNode) allocExact(current, want int, d *Datastore) {
	if want > current {
		for i := current; i < want; i++ {
			newChunkIndex := d.Alloc()
			l.Chunks[i] = ChunkIndex{byte(newChunkIndex >> 24), byte(newChunkIndex >> 16), byte(newChunkIndex >> 8), byte(newChunkIndex)}
		}
	} else if want < current {
		for i := want; i < current; i++ {
			d.Free(l.Chunks[i].AsInt())
			l.Chunks[i] = ChunkIndex{}
		}
	}
}

func (l *LeafNode) Free(d *Datastore) {
	l.allocExact(numChunks(int(l.KeyLen), int(l.ValueLen)), 0, d)
}
