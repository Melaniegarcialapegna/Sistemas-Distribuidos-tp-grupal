FNV32_OFFSET, FNV32_PRIME = 0x811C9DC5, 0x01000193

def fnv1a_32(data: bytes) -> int:           
        h = FNV32_OFFSET
        for byte in data:
            h ^= byte
            h = (h * FNV32_PRIME) & 0xFFFFFFFF
        return h