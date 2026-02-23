#!/usr/bin/env python3
"""Extract a named custom section from a Wasm binary to a file."""
import sys


def read_leb128(data, pos):
    result = 0
    shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, pos


def extract(wasm_path, section_name, output_path):
    with open(wasm_path, "rb") as f:
        data = f.read()

    pos = 8  # skip Wasm header (magic + version)
    while pos < len(data):
        section_id = data[pos]
        pos += 1
        size, pos = read_leb128(data, pos)
        section_start = pos

        if section_id == 0:  # custom section
            name_len, pos = read_leb128(data, pos)
            name = data[pos : pos + name_len].decode("utf-8")
            pos += name_len
            if name == section_name:
                with open(output_path, "wb") as out:
                    out.write(data[pos : section_start + size])
                return True

        pos = section_start + size
    return False


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <input.wasm> <section> <output>", file=sys.stderr)
        sys.exit(1)
    if not extract(sys.argv[1], sys.argv[2], sys.argv[3]):
        sys.exit(1)
