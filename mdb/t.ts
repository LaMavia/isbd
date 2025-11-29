#!/usr/bin/env bun
const filePath = process.argv[2];

if (filePath === undefined) {
  console.error("Usage: ./t.ts path_to_test_output");
  process.exit(1);
}

const file = Bun.file(filePath);

const lines = await file.text().then((t) => t.split("\n"));
const bufferLine = lines[1] ?? "";
const bytes = bufferLine.split(" ").filter(Boolean);

const bytesToInt = (bytes: string[]): number =>
  bytes.map(Number).reduce((u, b) => u * 255 + b, 0);

const collenOffsetBytes = bytes.slice(-8);
const collenOffset = bytesToInt(collenOffsetBytes);

const colOffsetBytes = bytes.slice(-16, -8);
const colOfffset = bytesToInt(colOffsetBytes);

const collenBytes = bytes.slice(collenOffset, -16);

const bufferSizeBytes = bytes.slice(0, 8);
const bufferSize = bytesToInt(bufferSizeBytes);

console.log({
  suggested_buffer_size: {
    bytes: bufferSizeBytes,
    value: bufferSize,
  },
  columns_offset: {
    bytes: colOffsetBytes.join(" "),
    value: colOfffset,
  },
  columns_lengths_offset: {
    bytes: collenOffsetBytes.join(" "),
    value: collenOffset,
    column_lengths: {
      bytes: collenBytes,
      len: collenBytes.length,
      expected_len: bytes.length - collenOffset - 16,
    },
  },
  outputLen: bytes.length,
});

/*
 
.

000 000 000 000 000 000 000 100 
000 000 000 000 000 000 000 010 010 000 000 002 002 002 002 002 002 002 002 002 001 002 002 002 002 
002 002 002 002 002 160 105 110 116 
049 001 105 110 116 050 001 005 000 
000 000 000 000 000 005 000 000 
000 000 000 000 000 005 000 000 

000 000 000 000 000 000 000 038 
000 000 000 000 000 000 000 049 

.
 */
