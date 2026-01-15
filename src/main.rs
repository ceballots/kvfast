use ptr_hash::{bucket_fn::CubicEps, PtrHash, PtrHashParams};

fn main() {
    let possible_objects = vec!["1", "2"];
    let phf: PtrHash<&str, CubicEps> = PtrHash::new(&possible_objects, PtrHashParams::default());

    for v in &possible_objects {
        let idx = phf.index(v);
        println!("Index for {}: {}", v, idx)
    }
}
