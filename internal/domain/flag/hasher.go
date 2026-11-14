package flag

// MaxBasisPoints represents 100.00% in basis points (1 basis point = 0.01%).
const MaxBasisPoints = 10000

const (
	c1 uint32 = 0xcc9e2d51
	c2 uint32 = 0x1b873593
)

// ComputeRolloutBucket deterministically maps a (flagKey, userID, seed) tuple
// to a bucket in the range [0, 9999] (basis points) using MurmurHash3 (x86_32).
//
// To guarantee 0 heap allocations on the hot evaluation path, keys under 256 bytes
// are concatenated on a stack-allocated array before hashing.
func ComputeRolloutBucket(flagKey, userID, seed string, hashSeed uint32) int {
	totalLen := len(flagKey) + 1 + len(userID)
	if len(seed) > 0 {
		totalLen += 1 + len(seed)
	}

	var stackBuf [256]byte
	var data []byte
	if totalLen <= len(stackBuf) {
		n := copy(stackBuf[:], flagKey)
		stackBuf[n] = ':'
		n++
		n += copy(stackBuf[n:], userID)
		if len(seed) > 0 {
			stackBuf[n] = ':'
			n++
			copy(stackBuf[n:], seed)
		}
		data = stackBuf[:totalLen]
	} else {
		data = make([]byte, totalLen)
		n := copy(data, flagKey)
		data[n] = ':'
		n++
		n += copy(data[n:], userID)
		if len(seed) > 0 {
			data[n] = ':'
			n++
			copy(data[n:], seed)
		}
	}

	h := MurmurHash3x86_32(data, hashSeed)
	return int(h % uint32(MaxBasisPoints))
}

// MurmurHash3x86_32 computes the 32-bit MurmurHash3 digest of data.
// Reference: Austin Appleby's canonical MurmurHash3 implementation.
func MurmurHash3x86_32(data []byte, seed uint32) uint32 {
	h1 := seed
	length := len(data)
	nblocks := length / 4

	for i := 0; i < nblocks; i++ {
		offset := i * 4
		k1 := uint32(data[offset]) |
			uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 |
			uint32(data[offset+3])<<24

		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xe6546b64
	}

	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(length)
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

