#!/usr/bin/env python3
"""Append a custom section to a Wasm module or component binary.

Usage: embed-wasm-section.py <wasm_file> <section_name> <data_file>

Modifies the wasm file in-place.
"""
import sys


def unsigned_leb128(n):
    result = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n != 0:
            byte |= 0x80
        result.append(byte)
        if n == 0:
            break
    return bytes(result)


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <wasm_file> <section_name> <data_file>",
              file=sys.stderr)
        sys.exit(1)

    wasm_path, section_name, data_path = sys.argv[1], sys.argv[2], sys.argv[3]

    with open(wasm_path, "rb") as f:
        wasm = f.read()
    with open(data_path, "rb") as f:
        data = f.read()

    if wasm[:4] != b"\x00asm":
        print(f"Error: {wasm_path} is not a valid Wasm binary", file=sys.stderr)
        sys.exit(1)

    # Custom section: id=0, then LEB128(payload_len), then LEB128(name_len) + name + data
    name_bytes = section_name.encode("utf-8")
    name_vec = unsigned_leb128(len(name_bytes)) + name_bytes
    payload = name_vec + data
    section = bytes([0]) + unsigned_leb128(len(payload)) + payload

    with open(wasm_path, "wb") as f:
        f.write(wasm + section)


if __name__ == "__main__":
    main()
