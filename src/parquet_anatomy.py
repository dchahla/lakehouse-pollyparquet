"""Look inside a Parquet file by reading its footer ourselves, no library.

Builds the same order-event shape the streaming path produces, writes it three ways with
pyarrow, then parses each file's footer by hand to report which columns compressed, which
didn't, and why. pyarrow writes the bytes; nothing but this file reads them back.

    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python3 src/parquet_anatomy.py --rows 100000

A Parquet file ends with a 4-byte little-endian footer length and the magic `PAR1`. Seek back
that far and you land on a Thrift *compact protocol* `FileMetaData` struct: the schema, the row
groups, and for every column chunk its encoding, its stats, and its size on disk. No page data
is decoded here; the footer alone answers "what did each column cost, and how was it stored?".

Two things worth watching. Parquet's defaults are not automatically the smallest answer, and
the random UUID the stream uses as its key takes up most of the file no matter what you do.
"""
import argparse, random, struct, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq   # WRITE only; the reader below is hand-rolled.

# Three ways to write identical rows. Encoding is a choice, and the default is only a default.
VARIANTS = {
    "defaults":  dict(compression="zstd"),
    "no-dict":   dict(compression="zstd", use_dictionary=False),
    # Delta encodings are Parquet v2 and opt-in; pyarrow will not reach for them on its own.
    # Prefix encoding needs the dictionary out of the way first.
    "delta-text": dict(compression="zstd", use_dictionary=False,
                       column_encoding={"event_id": "DELTA_BYTE_ARRAY",
                                        "ts": "DELTA_BYTE_ARRAY"}),
}

# Enums straight from parquet.thrift, so the footer's small integers become names.
ENCODINGS = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED",
             5: "DELTA_BINARY_PACKED", 6: "DELTA_LENGTH_BYTE_ARRAY",
             7: "DELTA_BYTE_ARRAY", 8: "RLE_DICTIONARY", 9: "BYTE_STREAM_SPLIT"}


# --- Thrift compact protocol: just enough to walk FileMetaData ------------------------------
# The compact protocol writes a struct as a stream of fields. Each field starts with one header
# byte: the high nibble is the field-id delta from the previous field, the low nibble is the
# wire type. A zero byte ends the struct. That is the whole grammar we need.
class Thrift:
    def __init__(self, buf: bytes):
        self.buf, self.pos = buf, 0

    def byte(self) -> int:
        b = self.buf[self.pos]; self.pos += 1; return b

    def varint(self) -> int:
        """Unsigned LEB128: 7 bits per byte, high bit means 'more follow'."""
        result = shift = 0
        while True:
            b = self.byte()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self) -> int:
        """Signed ints are zigzag-encoded so small magnitudes stay small."""
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def binary(self) -> bytes:
        n = self.varint()
        s = self.buf[self.pos:self.pos + n]; self.pos += n; return s

    def list_header(self) -> tuple[int, int]:
        """A list starts with one byte: high nibble = size (or 15 => varint), low nibble = elem type."""
        h = self.byte(); size = h >> 4
        if size == 15: size = self.varint()
        return size, h & 0x0F

    # Compact-protocol type ids (from thrift's TCompactProtocol):
    #  1 bool-true  2 bool-false  3 i8  4 i16  5 i32  6 i64  7 double
    #  8 binary/string  9 list  10 set  11 map  12 struct
    def skip(self, wire: int) -> None:
        """Advance past a value we don't care about, by wire type."""
        if wire in (1, 2):            return                 # bool: value is the type byte itself
        if wire in (3, 4, 5, 6):      self.varint(); return  # i8 / i16 / i32 / i64 (all varint)
        if wire == 7:                 self.pos += 8; return   # double
        if wire == 8:                 self.binary(); return   # binary / string
        if wire in (9, 10):                                   # list / set
            size, et = self.list_header()
            for _ in range(size): self.skip(et)
            return
        if wire == 11:                                        # map: header is size + key/val types
            size = self.varint()
            if size:
                kv = self.byte(); kt, vt = kv >> 4, kv & 0x0F
                for _ in range(size): self.skip(kt); self.skip(vt)
            return
        if wire == 12:                self.struct_skip(); return
        raise ValueError(f"unknown compact wire type {wire}")

    def struct_skip(self) -> None:
        self.fields(lambda fid, wire: self.skip(wire))

    def fields(self, visit) -> None:
        """Iterate one struct's fields, calling visit(field_id, wire_type) for each."""
        fid = 0
        while True:
            h = self.byte()
            if h == 0:                       # end-of-struct marker
                return
            delta, wire = h >> 4, h & 0x0F
            fid = fid + delta if delta else self.zigzag()   # delta==0 => explicit field id follows
            visit(fid, wire)


# --- FileMetaData walk ----------------------------------------------------------------------
# Field ids below come from parquet.thrift. We only pull what the report needs.
#   FileMetaData.row_groups = 4  (list<RowGroup>)
#   RowGroup.columns        = 1  (list<ColumnChunk>)
#   ColumnChunk.meta_data   = 3  (ColumnMetaData)
#   ColumnMetaData.encodings             = 2  (list<i32>)
#   ColumnMetaData.path_in_schema        = 3  (list<string>)
#   ColumnMetaData.total_compressed_size = 7  (i64)

def read_footer(path: Path) -> list[dict]:
    """Return one dict per column chunk (path, encodings, compressed bytes) from the footer alone."""
    with path.open("rb") as f:
        f.seek(-8, 2)                                  # last 8 bytes: [footer_len:4][PAR1]
        tail = f.read(8)
        if tail[4:] != b"PAR1":
            raise ValueError(f"{path} is not a Parquet file (no PAR1 magic at tail)")
        footer_len = struct.unpack("<I", tail[:4])[0]  # little-endian uint32
        f.seek(-(8 + footer_len), 2)
        footer = f.read(footer_len)

    chunks: list[dict] = []

    def on_column_meta(t: Thrift) -> dict:
        col = {"path": [], "encodings": [], "size": 0}
        def visit(fid, wire):
            if fid == 2 and wire == 9:                 # encodings: list<i32> (zigzag)
                size, _ = t.list_header()
                col["encodings"] = [ENCODINGS.get(t.zigzag(), "?") for _ in range(size)]
            elif fid == 3 and wire == 9:               # path_in_schema: list<string>
                size, _ = t.list_header()
                col["path"] = [t.binary().decode() for _ in range(size)]
            elif fid == 7:                             # total_compressed_size: i64 (zigzag)
                col["size"] = t.zigzag()
            else:
                t.skip(wire)
        t.fields(visit)
        return col

    def on_column_chunk(t: Thrift):
        def visit(fid, wire):
            if fid == 3 and wire == 12:                # meta_data: ColumnMetaData struct
                chunks.append(on_column_meta(t))
            else:
                t.skip(wire)
        t.fields(visit)

    def on_row_group(t: Thrift):
        def visit(fid, wire):
            if fid == 1 and wire == 9:                 # columns: list<ColumnChunk>
                h = t.byte(); size = h >> 4
                if size == 15: size = t.varint()
                for _ in range(size): on_column_chunk(t)
            else:
                t.skip(wire)
        t.fields(visit)

    def on_file_meta(t: Thrift):
        def visit(fid, wire):
            if fid == 4 and wire == 9:                 # row_groups: list<RowGroup>
                h = t.byte(); size = h >> 4
                if size == 15: size = t.varint()
                for _ in range(size): on_row_group(t)
            else:
                t.skip(wire)
        t.fields(visit)

    on_file_meta(Thrift(footer))
    return chunks


def stored_bytes(path: Path) -> dict[str, int]:
    """Bytes each column occupies on disk, summed across row groups. From our footer parse."""
    out: dict[str, int] = {}
    for c in read_footer(path):
        name = ".".join(c["path"])
        out[name] = out.get(name, 0) + c["size"]
    return out


def encodings(path: Path) -> dict[str, str]:
    """Encodings the writer recorded for each column, from the first row group's chunks."""
    seen: dict[str, str] = {}
    for c in read_footer(path):
        name = ".".join(c["path"])
        if name not in seen:
            seen[name] = ",".join(c["encodings"])
    return seen


def make_events(n: int) -> pa.Table:
    """The same four fields src/generate.py streams to Kafka."""
    start = datetime.now(timezone.utc) - timedelta(seconds=n * 0.5)
    return pa.table({
        "event_id":    [str(uuid.uuid4()) for _ in range(n)],           # unique, no shared prefixes
        "customer_id": [random.randint(1, 50) for _ in range(n)],       # 50 distinct values, always
        "amount":      [round(random.uniform(5, 500), 2) for _ in range(n)],
        "ts":          [(start + timedelta(seconds=i * 0.5)).isoformat() for i in range(n)],
    })


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--out", default="/tmp/parquet-anatomy")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    table = make_events(args.rows)

    files, sizes = {}, {}
    for name, opts in VARIANTS.items():
        p = out / f"{name}.parquet"
        pq.write_table(table, p, **opts)
        files[name], sizes[name] = p, stored_bytes(p)

    cols = list(sizes["defaults"])
    names = list(VARIANTS)

    print(f"\nStored bytes per column, {args.rows:,} rows. Same data every time.\n")
    print(f"  {'column':<14}" + "".join(f"{n:>14}" for n in names) + f"{'best':>14}")
    for c in cols:
        best = min(names, key=lambda n: sizes[n][c])
        print(f"  {c:<14}" + "".join(f"{sizes[n][c]:>14,}" for n in names) + f"{best:>14}")
    print(f"  {'TOTAL FILE':<14}" + "".join(f"{files[n].stat().st_size:>14,}" for n in names))

    # What share of the file is the identity column? On a UUID key this is the whole story.
    d = sizes["defaults"]
    big = max(cols, key=lambda c: d[c])
    print(f"\n{big} is {d[big] / sum(d.values()):.0%} of the default file.")

    dft, nod = files["defaults"].stat().st_size, files["no-dict"].stat().st_size
    verdict = "smaller" if dft < nod else "LARGER"
    print(f"Parquet's default (dictionary on) is {abs(dft - nod):,} bytes {verdict} than "
          f"turning the dictionary off.")

    print("\nEncodings actually used (read straight from the footer):")
    for name in names:
        print(f"  {name:<12} " + " | ".join(f"{c}={e}" for c, e in encodings(files[name]).items()))

# ==================================================================================================
# Glossary
#   pyarrow.parquet    Reference Parquet implementation; here it only WRITES the files.
#   Thrift compact     Parquet's footer serialization. Field header byte = (id-delta << 4 | wire).
#   varint / LEB128    Unsigned int, 7 bits per byte, high bit = continue. zigzag maps signed->small.
#   PAR1               Four-byte magic at both ends of the file; the tail one precedes the footer len.
#   footer length      Little-endian uint32 in the 4 bytes before the trailing PAR1.
#   FileMetaData       Root footer struct: schema, row_groups (fid 4), and file-level info.
#   RowGroup           A horizontal slice of the file; its columns (fid 1) are the column chunks.
#   ColumnChunk        One column within one row group; meta_data (fid 3) holds the details below.
#   path_in_schema     The column's name path (fid 5). Flat schemas make this a single element.
#   encodings          The encodings the writer used for this chunk (fid 8), as parquet.thrift enums.
#   total_compressed_size  On-disk bytes for the chunk after encoding AND compression (fid 7).
#   RLE_DICTIONARY     Store unique values once, replace occurrences with small integer keys.
#   DELTA_BYTE_ARRAY   Prefix encoding: shared prefix length plus the differing suffix. Opt-in.
#   PLAIN              No encoding. The fallback when nothing else pays off.
# ==================================================================================================
