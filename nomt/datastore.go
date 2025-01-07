package nomt

const (
	MaxChunks = 1 << 28
	ChunkSize = 64
)

type Datastore struct {
	Data        [MaxChunks][ChunkSize]byte
	FreeList    [MaxChunks]uint32
	FreeListIdx int
}

func New() *Datastore {
	datastore := &Datastore{}
	for i := uint32(0); i < MaxChunks; i++ {
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
