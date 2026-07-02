use rand::Rng;
use std::process;

/// Generate random letters of specified length (equivalent to Go's GenerateRandomLetters)
pub fn generate_random_letters(length: usize) -> String {
    let mut rng = rand::thread_rng();
    const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    (0..length)
        .map(|_| {
            let idx = rng.gen_range(0..LETTERS.len());
            LETTERS[idx] as char
        })
        .collect()
}

/// Split x-hops string (equivalent to Go's splitHops)
pub fn split_hops(hops_str: &str) -> Vec<String> {
    hops_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Get current process ID
pub fn get_pid() -> u32 {
    process::id()
}