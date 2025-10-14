use std::collections::HashSet;
use std::hash::Hash;

pub fn compare_vecs<T: PartialEq>(vec1: &[T], vec2: &[T]) -> bool {
    vec1.len() == vec2.len() && vec1.iter().zip(vec2.iter()).all(|(a, b)| a == b)
}

pub fn compare_vecs_unordered<T: Eq + Hash + Clone>(vec1: &[T], vec2: &[T]) -> bool {
    let set1: HashSet<_> = vec1.iter().cloned().collect();
    let set2: HashSet<_> = vec2.iter().cloned().collect();

    set1 == set2
}
