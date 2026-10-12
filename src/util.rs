use std::path::{Path, PathBuf};

/// Parses sizes like `1048576`, `512K`, `1M`, `2G`.
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('K' | 'k') => (&s[..s.len() - 1], 1u64 << 10),
        Some('M' | 'm') => (&s[..s.len() - 1], 1 << 20),
        Some('G' | 'g') => (&s[..s.len() - 1], 1 << 30),
        _ => (s, 1),
    };
    num.parse::<u64>()
        .map_err(|e| format!("invalid size {s}: {e}"))?
        .checked_mul(mult)
        .ok_or_else(|| format!("size too large: {s}"))
}

/// Longest file name produced, in bytes. Leaves room under the usual 255-byte
/// limit for `.part.state` and a `.N` collision suffix.
const MAX_NAME: usize = 200;

/// File name from the last URL path segment, or `download`.
pub fn file_name_from_url(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or("");
    let after_scheme = path.split_once("://").map_or(path, |(_, rest)| rest);
    let raw = after_scheme
        .split_once('/')
        .and_then(|(_, p)| p.rsplit('/').next())
        .unwrap_or("");
    let name = percent_decode(raw);
    // Nothing decoded may act as a separator, a terminator or a control code.
    let name: String = name
        .chars()
        .map(|c| {
            if c == '/' || c == '\\' || c.is_control() {
                '_'
            } else {
                c
            }
        })
        .collect();
    let name = truncate_name(name.trim());
    match name.as_str() {
        "" | "." | ".." => "download".to_string(),
        _ => name,
    }
}

/// Decodes `%XX` escapes; the result is used only if it is valid UTF-8.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = |c: u8| (c as char).to_digit(16);
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

/// Cuts `name` to `MAX_NAME` bytes on a character boundary, keeping a short
/// extension.
fn truncate_name(name: &str) -> String {
    if name.len() <= MAX_NAME {
        return name.to_string();
    }
    let ext = match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && ext.len() <= 16 => &name[stem.len()..],
        _ => "",
    };
    let mut cut = MAX_NAME - ext.len();
    while !name.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{ext}", &name[..cut])
}

/// `dir/name`, or `dir/stem.N.ext` when that path (or its partial file) is taken.
pub fn unique_path(dir: &Path, name: &str, taken: impl Fn(&Path) -> bool) -> PathBuf {
    let first = dir.join(name);
    if !taken(&first) {
        return first;
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s, Some(e)),
        _ => (name, None),
    };
    (1..)
        .map(|n| match ext {
            Some(e) => dir.join(format!("{stem}.{n}.{e}")),
            None => dir.join(format!("{stem}.{n}")),
        })
        .find(|p| !taken(p))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(file_name_from_url("http://h/a/b.iso?x=1"), "b.iso");
        assert_eq!(file_name_from_url("http://h/"), "download");
        assert_eq!(file_name_from_url("http://h"), "download");
        assert_eq!(file_name_from_url("http://h/a/.."), "download");
        assert_eq!(file_name_from_url("http://h/my%20file.zip"), "my file.zip");
        assert_eq!(
            file_name_from_url("http://h/%E6%97%A5%E6%9C%AC.txt"),
            "日本.txt"
        );
        // Decoded separators and controls never reach the file system.
        assert_eq!(file_name_from_url("http://h/a%2F..%2Fb"), "a_.._b");
        assert_eq!(file_name_from_url("http://h/%2e%2e"), "download");
        assert_eq!(file_name_from_url("http://h/x%00y%0A"), "x_y_");
        // Not UTF-8 once decoded: keep the escapes.
        assert_eq!(file_name_from_url("http://h/%FF.bin"), "%FF.bin");
        assert_eq!(file_name_from_url("http://h/100%"), "100%");
        let long = format!("http://h/{}.iso", "あ".repeat(100));
        let name = file_name_from_url(&long);
        assert!(name.len() <= MAX_NAME && name.ends_with(".iso"), "{name}");
    }

    #[test]
    fn unique() {
        let taken = |p: &Path| p.ends_with("f.bin") || p.ends_with("f.1.bin");
        assert_eq!(
            unique_path(Path::new("/d"), "f.bin", taken),
            Path::new("/d/f.2.bin")
        );
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("1M"), Ok(1 << 20));
        assert_eq!(parse_size("512k"), Ok(512 << 10));
        assert!(parse_size("x").is_err());
    }
}
