package nomt

const (
	MaxChunks = 1 << 24
	ChunkSize = 32
)

type Datastore struct {
	Data        [MaxChunks][ChunkSize]byte
	FreeList    [MaxChunks]int32
	FreeListIdx int
}

func New() *Datastore {
	datastore := &Datastore{}
	for i := 0; i < MaxChunks; i++ {
		datastore.Free(i)
	}
	return datastore
}

func (d *Datastore) Free(idx int) {
	d.FreeList[d.FreeListIdx] = int32(idx)
	d.FreeListIdx++
}

func (d *Datastore) Alloc() int {
	d.FreeListIdx--
	return int(d.FreeList[d.FreeListIdx])
}
