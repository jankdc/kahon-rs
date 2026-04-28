# kahon

A Rust writer for the [Kahon binary format](https://github.com/jankdc/kahon).

## Goals

- Values are written as they arrive; containers reference children by back-offset. Memory stays bounded by tree depth, not document size.
- Arrays and objects are B+trees on disk: index into a million-element array, or look up a key in a million-key object, without scanning.
- Fixed-fanout for tight in-memory output, or page-aligned target-byte sizing for files that will be `pread`-ed or memory-mapped.
- Uses flexbuffer-like builder pattern API to construct the JSON object.

## Usage

```
cargo add kahon
```

```rust
use kahon::Writer;

let mut buf: Vec<u8> = Vec::new();
let mut w = Writer::new(&mut buf);

let mut monster = w.start_object();
monster.push_i64("hp", 80)?;
monster.push_bool("enraged", true)?;

{
    let mut weapons = monster.start_array("weapons")?;
    weapons.push_str("fist")?;
    let mut axe = weapons.start_object();
    axe.push_str("name", "great axe")?;
    axe.push_i64("damage", 15)?;
    // axe and weapons auto-close on drop
}

monster.end()?; // explicit close propagates errors
w.finish()?;    // writes the 12-byte trailer
```

Builders close themselves on `Drop`; use `.end()` when you want errors surfaced rather than swallowed into the writer's poison flag.

## Tuning

```rust
use kahon::{Writer, WriterOptions, BuildPolicy};

// Disk-tuned: each B+tree node targets one page, trailer is page-aligned.
let opts = WriterOptions {
    policy: BuildPolicy::disk_aligned(4096),
    ..Default::default()
};
let w = Writer::with_options(file, opts)?;
```

The default (`BuildPolicy::compact(128)`) produces the tightest output and is best for in-memory or network use. `disk_aligned` trades a small amount of unreferenced padding for layout that plays well with the page cache.

## Status

Pre-1.0. The on-disk format is governed by an [external spec](https://github.com/jankdc/kahon); comments in the source cite section numbers. A reference reader lives under `tests/common/` and is exercised by the golden-file and property tests.

## License

All source code is licensed under MIT.

All contributions are to be licensed as MIT.
