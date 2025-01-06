package nomt

import (
	"bytes"
)

type (
	Node       [32]byte
	ChunkIndex [3]byte
)

func (c *ChunkIndex) AsInt() int {
	return int(c[0])<<16 | int(c[1])<<8 | int(c[2])
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
	Chunks   [9]ChunkIndex
	_        [2]byte // ignored
}

func (l *LeafNode) Put(key, value []byte, db *Datastore) {
	pos, chunk := 0, 0
	for pos < len(key) {
		last := pos + ChunkSize
		if last > len(key) {
			last = len(key)
		} else {
			chunk++
		}
		pos = pos + copy(db.Data[l.Chunks[chunk].AsInt()][:], key[pos:last])
	}

	for pos < len(key)+len(value) {
		last := pos + ChunkSize
		if last > len(key)+len(value) {
			last = len(key) + len(value)
		} else {
			chunk++
		}
		pos = pos + copy(db.Data[l.Chunks[chunk].AsInt()][:], value[pos-len(key):last-len(key)])
	}
}

func (l *LeafNode) get(buf []byte, startChunk int, length int, db *Datastore) {
	pos, chunk := 0, startChunk
	for pos < length {
		last := pos + ChunkSize
		if last > length {
			last = length
		} else {
			chunk++
		}
		pos = pos + copy(buf[pos:last], db.Data[l.Chunks[chunk].AsInt()][:])
	}
}

func (l *LeafNode) put(buf []byte, startChunk int, length int, db *Datastore) {
	pos, chunk := 0, startChunk
	for pos < length {
		last := pos + ChunkSize
		if last > length {
			last = length
		} else {
			chunk++
		}
		pos = pos + copy(db.Data[l.Chunks[chunk].AsInt()][:], buf[pos:last])
	}
}

func (l *LeafNode) valueStartChunk() int {
	return (int(l.KeyLen) + ChunkSize - 1) / ChunkSize
}

func (l *LeafNode) GetKey(buf []byte, db *Datastore) []byte {
	l.get(buf, 0, int(l.KeyLen), db)
	return buf[:l.KeyLen]
}

func (l *LeafNode) GetValue(buf []byte, db *Datastore) []byte {
	l.get(buf, l.valueStartChunk(), int(l.ValueLen), db)
	return buf[:l.ValueLen]
}

func (l *LeafNode) PutValue(value []byte, db *Datastore) {
	l.allocExact(
		numChunks(int(l.KeyLen), int(l.ValueLen)),
		numChunks(int(l.KeyLen), len(value)),
		db,
	)
	l.ValueLen = byte(len(value))
	l.put(value, l.valueStartChunk(), len(value), db)
}

func (l *LeafNode) PutKeyValue(key, value []byte, db *Datastore) {
	l.allocExact(
		numChunks(int(l.KeyLen), int(l.ValueLen)),
		numChunks(len(key), len(value)),
		db,
	)
	l.KeyLen = byte(len(key))
	l.ValueLen = byte(len(value))

	l.put(key, 0, len(key), db)
	l.put(value, l.valueStartChunk(), len(value), db)
}

func numChunks(keyLen, valueLen int) int {
	keyChunks := (keyLen + ChunkSize - 1) / ChunkSize
	valueChunks := (valueLen + ChunkSize - 1) / ChunkSize
	return keyChunks + valueChunks
}

func (l *LeafNode) allocExact(current, want int, d *Datastore) {
	if want > current {
		for i := current; i < want; i++ {
			newChunkIndex := d.Alloc()
			l.Chunks[i] = ChunkIndex{byte(newChunkIndex >> 16), byte(newChunkIndex >> 8), byte(newChunkIndex)}
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
