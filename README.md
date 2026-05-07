# Kahon for Rust

A Rust writer for the [Kahon binary format](https://github.com/jankdc/kahon).


## Usage

```
cargo add kahon
```

### Builder API

```rust
use kahon::Writer;

let mut buf: Vec<u8> = Vec::new();
let w = Writer::new(&mut buf);

// `start_object` consumes the empty `Writer` and returns a root builder.
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

// `.end()` returns the writer in its `Filled` state, ready for `.finish()`.
let w = monster.end()?;
w.finish()?;
```

> The builder API uses *typestate* to enforce one root container at compile time.

### Advanced Use Cases

For more advanced use cases, we have the `raw::RawWriter`. Mismatched frames and stray keys surface as runtime
errors instead.

```rust
use kahon::raw::RawWriter;

let mut buf: Vec<u8> = Vec::new();
let mut w = RawWriter::new(&mut buf);

w.begin_object()?;
w.push_key("hp")?;
w.push_i64(80)?;
w.push_key("enraged")?;
w.push_bool(true)?;

w.push_key("weapons")?;
w.begin_array()?;
w.push_str("fist")?;
w.begin_object()?;
w.push_key("name")?;
w.push_str("great axe")?;
w.push_key("damage")?;
w.push_i64(15)?;
w.end_object()?;
w.end_array()?;

w.end_object()?;
w.finish()?;
```

## Features

- **Memory Friendly**: Memory can stay bounded by how deep your JSON is, not document size.
- **Fast Value Retrieval**: Similar to SQLite, arrays and objects are stored as B+trees: index into a million-element array, or look up a key in a million-key object, without scanning for the whole thing first.
- **Tuneable**: There's a bunch of options to balance out memory usage and read performance to your liking.

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

Pre-1.0. Stuff is still moving and breaking quite a lot due to having to play with the API so there's no guarantee of stability at the moment. The on-disk format is governed by an [external spec](https://github.com/jankdc/kahon); comments in the source cite section numbers. A reference reader lives under `tests/common/` and is exercised by the golden-file and property tests.

## License

All source code is licensed under MIT.

All contributions are to be licensed as MIT.
