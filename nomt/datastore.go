package nomt

const (
	MaxChunks = 1 << 32
	ChunkSize = 64
)

type Datastore struct {
	Data        [MaxChunks][ChunkSize]byte
	FreeList    [MaxChunks]uint32
	FreeListIdx int
}

func New() *Datastore {
	datastore := &Datastore{}
	for i := uint32(0); i < i+1; i++ { // Go to max uint32
		datastore.Free(i)
	}
	return datastore
}

func (d *Datastore) Free(idx uint32) {
	d.FreeList[d.FreeListIdx] = idx
	d.FreeListIdx++
}

func (d *Datastore) Alloc() uint32 {
	d.FreeListIdx--
	return d.FreeList[d.FreeListIdx]
}
