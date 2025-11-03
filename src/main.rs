use boomphf::*;
// Generate MPHF

fn main() {
    let possible_objects = vec!["1", "2"];
    let n = possible_objects.len();
    let phf = Mphf::new(1.7, &possible_objects);

    for v in &possible_objects {
        let idx = phf.hash(v);
        println!("here {idx}")
    }
}
