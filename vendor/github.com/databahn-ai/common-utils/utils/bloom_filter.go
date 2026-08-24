package utils

import (
	"math"
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

type BloomFilter struct {
	SizeBits    uint32
	HashCount   uint32
	ByteSize    uint32
	Bits        []byte
	InsertCount uint32
}

func NewBloomFilter(sizeBits, hashCount uint32) *BloomFilter {
	byteSize := (sizeBits + 7) / 8

	return &BloomFilter{
		SizeBits:  sizeBits,
		HashCount: hashCount,
		ByteSize:  byteSize,
		Bits:      make([]byte, byteSize),
	}
}

func OptimalSize(expectedEntries int, fpRate float64) (sizeBits uint32, hashCount uint32) {
	m := math.Ceil(
		-float64(expectedEntries) * math.Log(fpRate) /
			(math.Log(2) * math.Log(2)),
	)

	k := math.Max(
		1,
		math.Round((m/float64(expectedEntries))*math.Log(2)),
	)

	return uint32(m), uint32(k)
}

func (bf *BloomFilter) hashes(item string) []uint32 {
	data := []byte(item)

	h1 := xxhash.Sum64(data)
	h2 := xxhash.Sum64(append([]byte{1}, data...))

	if h2 == 0 {
		h2 = 1
	}

	hashes := make([]uint32, bf.HashCount)

	for i := uint32(0); i < bf.HashCount; i++ {
		hashes[i] = uint32(
			(h1 + uint64(i)*h2) % uint64(bf.SizeBits),
		)
	}

	return hashes
}

func (bf *BloomFilter) Add(item string) {
	for _, pos := range bf.hashes(item) {
		byteIdx := pos / 8
		bitIdx := pos % 8

		bf.Bits[byteIdx] |= 1 << bitIdx
	}

	bf.InsertCount++
}

func (bf *BloomFilter) Contains(item string) bool {
	for _, pos := range bf.hashes(item) {
		byteIdx := pos / 8
		bitIdx := pos % 8

		if bf.Bits[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) OccupancyRatio() float64 {
	var setBits int

	for _, b := range bf.Bits {
		setBits += bits.OnesCount8(b)
	}

	if bf.SizeBits == 0 {
		return 0
	}

	return float64(setBits) / float64(bf.SizeBits)
}

func (bf *BloomFilter) EstimatedFPRate() float64 {
	n := float64(max(uint32(1), bf.InsertCount))
	m := float64(bf.SizeBits)
	k := float64(bf.HashCount)

	return math.Pow(
		1-math.Exp(-(k*n)/m),
		k,
	)
}
