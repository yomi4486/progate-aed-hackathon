from __future__ import annotations

import copy
import hashlib
import math
import sys
from io import BytesIO
from struct import calcsize, pack, unpack
from typing import Any, BinaryIO, Callable, ClassVar, Iterator

import bitarray
import xxhash


def _range_fn(start: int = 0, stop: int | None = None) -> Iterator[int]:
    return iter(range(start, stop if stop is not None else sys.maxsize))


def _is_string_io(instance: object) -> bool:
    return isinstance(instance, BytesIO)


def make_hashfuncs(num_slices: int, num_bits: int) -> tuple[Callable[[object], Iterator[int]], Any]:
    if num_bits >= (1 << 31):
        fmt_code, chunk_size = "Q", 8
    elif num_bits >= (1 << 15):
        fmt_code, chunk_size = "I", 4
    else:
        fmt_code, chunk_size = "H", 2
    total_hash_bits = 8 * num_slices * chunk_size
    if total_hash_bits > 384:
        hashfn = hashlib.sha512
    elif total_hash_bits > 256:
        hashfn = hashlib.sha384
    elif total_hash_bits > 160:
        hashfn = hashlib.sha256
    elif total_hash_bits > 128:
        hashfn = hashlib.sha1
    else:
        hashfn = xxhash.xxh128

    fmt = fmt_code * (hashfn().digest_size // chunk_size)
    num_salts, extra = divmod(num_slices, len(fmt))
    if extra:
        num_salts += 1
    salts = tuple(hashfn(hashfn(pack("I", i)).digest()) for i in _range_fn(0, num_salts))

    def _hash_maker(key: object) -> Iterator[int]:
        if isinstance(key, str):
            key = key.encode("utf-8")
        else:
            key = str(key).encode("utf-8")
        i = 0
        for salt in salts:
            h = salt.copy()
            h.update(key)
            for uint in unpack(fmt, h.digest()):
                yield uint % num_bits
                i += 1
                if i >= num_slices:
                    return

    return _hash_maker, hashfn


class BloomFilter(object):
    FILE_FMT: ClassVar[str] = "<dQQQQ"

    # Public attributes (initialized in _setup or __init__)
    error_rate: float
    num_slices: int
    bits_per_slice: int
    capacity: int
    num_bits: int
    count: int
    make_hashes: Callable[[object], Iterator[int]]
    hashfn: Any
    bitarray: bitarray.bitarray

    def __init__(self, capacity: int, error_rate: float = 0.001) -> None:
        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")
        if not capacity > 0:
            raise ValueError("Capacity must be > 0")
        # given M = num_bits, k = num_slices, P = error_rate, n = capacity
        #       k = log2(1/P)
        # solving for m = bits_per_slice
        # n ~= M * ((ln(2) ** 2) / abs(ln(P)))
        # n ~= (k * m) * ((ln(2) ** 2) / abs(ln(P)))
        # m ~= n * abs(ln(P)) / (k * (ln(2) ** 2))
        num_slices = int(math.ceil(math.log(1.0 / error_rate, 2)))
        bits_per_slice = int(math.ceil((capacity * abs(math.log(error_rate))) / (num_slices * (math.log(2) ** 2))))
        self._setup(error_rate, num_slices, bits_per_slice, capacity, 0)
        self.bitarray = bitarray.bitarray(self.num_bits, endian="little")
        self.bitarray.setall(False)

    def _setup(
        self,
        error_rate: float,
        num_slices: int,
        bits_per_slice: int,
        capacity: int,
        count: int,
    ) -> None:
        self.error_rate = error_rate
        self.num_slices = num_slices
        self.bits_per_slice = bits_per_slice
        self.capacity = capacity
        self.num_bits = num_slices * bits_per_slice
        self.count = count
        self.make_hashes, self.hashfn = make_hashfuncs(self.num_slices, self.bits_per_slice)

    def __contains__(self, key: object) -> bool:
        """Tests a key's membership in this bloom filter."""
        bits_per_slice = self.bits_per_slice
        bitarray = self.bitarray
        hashes = self.make_hashes(key)
        offset = 0
        for k in hashes:
            if not bitarray[offset + k]:
                return False
            offset += bits_per_slice
        return True

    def __len__(self) -> int:
        return self.count

    def add(self, key: object, skip_check: bool = False) -> bool:
        bitarray = self.bitarray
        bits_per_slice = self.bits_per_slice
        hashes = self.make_hashes(key)
        found_all_bits = True
        if self.count > self.capacity:
            raise IndexError("BloomFilter is at capacity")
        offset = 0
        for k in hashes:
            if not skip_check and found_all_bits and not bitarray[offset + k]:
                found_all_bits = False
            self.bitarray[offset + k] = True
            offset += bits_per_slice

        if skip_check:
            self.count += 1
            return False
        elif not found_all_bits:
            self.count += 1
            return False
        else:
            return True

    def copy(self) -> "BloomFilter":
        new_filter: BloomFilter = BloomFilter(self.capacity, self.error_rate)
        new_filter.bitarray = self.bitarray.copy()
        return new_filter

    def union(self, other: "BloomFilter") -> "BloomFilter":
        if self.capacity != other.capacity or self.error_rate != other.error_rate:
            raise ValueError("Unioning filters requires both filters to have both the same capacity and error rate")
        new_bloom = self.copy()
        new_bloom.bitarray = new_bloom.bitarray | other.bitarray
        return new_bloom

    def __or__(self, other: "BloomFilter") -> "BloomFilter":
        return self.union(other)

    def intersection(self, other: "BloomFilter") -> "BloomFilter":
        if self.capacity != other.capacity or self.error_rate != other.error_rate:
            raise ValueError("Intersecting filters requires both filters to have equal capacity and error rate")
        new_bloom = self.copy()
        new_bloom.bitarray = new_bloom.bitarray & other.bitarray
        return new_bloom

    def __and__(self, other: "BloomFilter") -> "BloomFilter":
        return self.intersection(other)

    def tofile(self, f: BinaryIO) -> None:
        f.write(pack(self.FILE_FMT, self.error_rate, self.num_slices, self.bits_per_slice, self.capacity, self.count))
        (f.write(self.bitarray.tobytes()) if _is_string_io(f) else self.bitarray.tofile(f))

    @classmethod
    def fromfile(cls, f: BinaryIO, n: int = -1) -> "BloomFilter":
        headerlen: int = calcsize(cls.FILE_FMT)

        if 0 < n < headerlen:
            raise ValueError("n too small!")

        filter = cls(1)  # Bogus instantiation, we will `_setup'.
        unpacked = unpack(cls.FILE_FMT, f.read(headerlen))
        filter._setup(*unpacked)
        filter.bitarray = bitarray.bitarray(endian="little")
        if n > 0:
            (
                filter.bitarray.frombytes(f.read(n - headerlen))
                if _is_string_io(f)
                else filter.bitarray.fromfile(f, n - headerlen)
            )
        else:
            (filter.bitarray.frombytes(f.read()) if _is_string_io(f) else filter.bitarray.fromfile(f))
        if filter.num_bits != len(filter.bitarray) and (
            filter.num_bits + (8 - filter.num_bits % 8) != len(filter.bitarray)
        ):
            raise ValueError("Bit length mismatch!")

        return filter

    def __getstate__(self) -> dict[str, Any]:
        d = self.__dict__.copy()
        del d["make_hashes"]
        return d

    def __setstate__(self, d: dict[str, Any]) -> None:
        self.__dict__.update(d)
        self.make_hashes, self.hashfn = make_hashfuncs(self.num_slices, self.bits_per_slice)


class ScalableBloomFilter(object):
    SMALL_SET_GROWTH = 2  # slower, but takes up less memory
    LARGE_SET_GROWTH = 4  # faster, but takes up more memory faster
    FILE_FMT: ClassVar[str] = "<idQd"

    scale: int
    ratio: float
    initial_capacity: int
    error_rate: float
    filters: list[BloomFilter]

    def __init__(self, initial_capacity: int = 100, error_rate: float = 0.001, mode: int = LARGE_SET_GROWTH) -> None:
        if not error_rate or error_rate < 0:
            raise ValueError("Error_Rate must be a decimal less than 0.")
        self._setup(mode, 0.9, initial_capacity, error_rate)
        self.filters = []

    def _setup(self, mode: int, ratio: float, initial_capacity: int, error_rate: float) -> None:
        self.scale = mode
        self.ratio = ratio
        self.initial_capacity = initial_capacity
        self.error_rate = error_rate

    def __contains__(self, key: object) -> bool:
        for f in reversed(self.filters):
            if key in f:
                return True
        return False

    def add(self, key: object) -> bool:
        if key in self:
            return True
        if not self.filters:
            filter = BloomFilter(capacity=self.initial_capacity, error_rate=self.error_rate * self.ratio)
            self.filters.append(filter)
        else:
            filter = self.filters[-1]
            if filter.count >= filter.capacity:
                filter = BloomFilter(capacity=filter.capacity * self.scale, error_rate=filter.error_rate * self.ratio)
                self.filters.append(filter)
        filter.add(key, skip_check=True)
        return False

    def union(self, other: "ScalableBloomFilter") -> "ScalableBloomFilter":
        if (
            self.scale != other.scale
            or self.initial_capacity != other.initial_capacity
            or self.error_rate != other.error_rate
        ):
            raise ValueError(
                "Unioning two scalable bloom filters requires \
            both filters to have both the same mode, initial capacity and error rate"
            )
        if len(self.filters) > len(other.filters):
            larger_sbf = copy.deepcopy(self)
            smaller_sbf = other
        else:
            larger_sbf = copy.deepcopy(other)
            smaller_sbf = self
        # Union the underlying classic bloom filters
        new_filters: list[BloomFilter] = []
        for i in range(len(smaller_sbf.filters)):
            new_filter = larger_sbf.filters[i] | smaller_sbf.filters[i]
            new_filters.append(new_filter)
        for i in range(len(smaller_sbf.filters), len(larger_sbf.filters)):
            new_filters.append(larger_sbf.filters[i])
        larger_sbf.filters = new_filters
        return larger_sbf

    def __or__(self, other: "ScalableBloomFilter") -> "ScalableBloomFilter":
        return self.union(other)

    @property
    def capacity(self) -> int:
        return sum(f.capacity for f in self.filters)

    @property
    def count(self) -> int:
        return len(self)

    def tofile(self, f: BinaryIO) -> None:
        f.write(pack(self.FILE_FMT, self.scale, self.ratio, self.initial_capacity, self.error_rate))

        # Write #-of-filters
        f.write(pack("<l", len(self.filters)))

        if len(self.filters) > 0:
            # Then each filter directly, with a header describing
            # their lengths.
            headerpos = f.tell()
            headerfmt = "<" + "Q" * (len(self.filters))
            f.write(b"." * calcsize(headerfmt))
            filter_sizes: list[int] = []
            for filter in self.filters:
                begin = f.tell()
                filter.tofile(f)
                filter_sizes.append(f.tell() - begin)

            f.seek(headerpos)
            f.write(pack(headerfmt, *filter_sizes))

    @classmethod
    def fromfile(cls, f: BinaryIO) -> "ScalableBloomFilter":
        filter = cls()
        filter._setup(*unpack(cls.FILE_FMT, f.read(calcsize(cls.FILE_FMT))))
        (nfilters,) = unpack("<l", f.read(calcsize("<l")))
        if nfilters > 0:
            header_fmt = "<" + "Q" * nfilters
            bytes = f.read(calcsize(header_fmt))
            filter_lengths = unpack(header_fmt, bytes)
            for fl in filter_lengths:
                filter.filters.append(BloomFilter.fromfile(f, fl))
        else:
            filter.filters = []

        return filter

    def __len__(self) -> int:
        return sum(f.count for f in self.filters)
