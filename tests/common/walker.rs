//! Walks an emitted Kahon tree to assert spec §8 invariant 3: every
//! internal B+tree node must reference m >= 2 children. Recurses into
//! arrays/objects; treats scalars, strings, and empty-singleton containers
//! as terminals.

fn read_varuint(buf: &[u8], pos: &mut usize) -> u64 {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        let b = buf[*pos];
        *pos += 1;
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return v;
        }
        shift += 7;
    }
}

fn read_uintw(buf: &[u8], pos: &mut usize, width: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes[..width].copy_from_slice(&buf[*pos..*pos + width]);
    *pos += width;
    u64::from_le_bytes(bytes)
}

pub fn assert_no_m_below_2(buf: &[u8], root_off: u64) {
    walk(buf, root_off as usize);
}

fn walk(buf: &[u8], off: usize) {
    let tag = buf[off];
    let high = tag & 0xFC;
    let w = (tag & 0x03) as usize;
    let width = 1usize << w;

    match high {
        0x70 => {
            // array leaf: tag + varuint n + n × uintW offsets
            let mut p = off + 1;
            let n = read_varuint(buf, &mut p);
            for _ in 0..n {
                let child = read_uintw(buf, &mut p, width);
                walk(buf, child as usize);
            }
        }
        0x74 => {
            // array internal: tag + varuint total + varuint m + m × (u64_le sub_total, uintW off)
            let mut p = off + 1;
            let _total = read_varuint(buf, &mut p);
            let m = read_varuint(buf, &mut p);
            assert!(
                m >= 2,
                "array-internal node at offset {} has m={} (spec §8 inv. 3 forbids m<2)",
                off,
                m
            );
            for _ in 0..m {
                p += 8; // sub_total
                let child = read_uintw(buf, &mut p, width);
                walk(buf, child as usize);
            }
        }
        0x80 => {
            // object leaf: tag + varuint n + n × (key_off, val_off)
            let mut p = off + 1;
            let n = read_varuint(buf, &mut p);
            for _ in 0..n {
                let k = read_uintw(buf, &mut p, width);
                let v = read_uintw(buf, &mut p, width);
                walk(buf, k as usize);
                walk(buf, v as usize);
            }
        }
        0x84 => {
            // object internal: tag + varuint total + varuint m
            //                + m × (u64_le sub_total, uintW key_lo_off,
            //                       uintW key_hi_off, uintW node_off)
            let mut p = off + 1;
            let _total = read_varuint(buf, &mut p);
            let m = read_varuint(buf, &mut p);
            assert!(
                m >= 2,
                "object-internal node at offset {} has m={} (spec §8 inv. 3 forbids m<2)",
                off,
                m
            );
            for _ in 0..m {
                p += 8; // sub_total
                let _key_lo = read_uintw(buf, &mut p, width);
                let _key_hi = read_uintw(buf, &mut p, width);
                let child = read_uintw(buf, &mut p, width);
                walk(buf, child as usize);
            }
        }
        _ => { /* scalar / string / empty container — terminal */ }
    }
}
