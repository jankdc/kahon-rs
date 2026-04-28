use kahon::Writer;

fn main() {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = Writer::new(&mut buf);
        {
            let mut monster = w.start_object();
            monster.push_i64("hp", 80).unwrap();
            monster.push_i64("mana", 200).unwrap();
            monster.push_bool("enraged", true).unwrap();

            {
                let mut weapons = monster.start_array("weapons").unwrap();
                weapons.push_str("fist").unwrap();

                {
                    let mut axe = weapons.start_object();
                    axe.push_str("name", "great axe").unwrap();
                    axe.push_i64("damage", 15).unwrap();
                    // auto-close on drop
                }

                {
                    let mut hammer = weapons.start_object();
                    hammer.push_str("name", "hammer").unwrap();
                    hammer.push_i64("damage", 5).unwrap();
                }
                // auto-close weapons
            }

            {
                let mut coins = monster.start_array("coins").unwrap();
                for c in [5i64, 10, 25, 25, 25, 100] {
                    coins.push_i64(c).unwrap();
                }
            }

            {
                let mut position = monster.start_array("position").unwrap();
                for _ in 0..3 {
                    position.push_f64(0.0).unwrap();
                }
            }

            monster.end().unwrap();
        }
        w.finish().unwrap();
    }

    println!("monster serialized in {} bytes", buf.len());
}
