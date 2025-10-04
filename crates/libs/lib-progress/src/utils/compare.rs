pub fn compare_vecs<T: PartialEq>(vec1: &[T], vec2: &[T]) -> bool {
    vec1.len() == vec2.len() && vec1.iter().zip(vec2.iter()).all(|(a, b)| a == b)
}
