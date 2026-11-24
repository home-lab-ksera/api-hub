//! 先頭バイトによるファイル形式の確認。Content-Type だけを信用しないために使う。

pub fn is_pdf(data: &[u8]) -> bool {
    data.starts_with(b"%PDF-")
}

pub fn is_png(data: &[u8]) -> bool {
    data.starts_with(b"\x89PNG\r\n\x1a\n")
}

pub fn is_jpeg(data: &[u8]) -> bool {
    data.starts_with(b"\xff\xd8\xff")
}

pub fn is_gif(data: &[u8]) -> bool {
    data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a")
}

pub fn is_webp(data: &[u8]) -> bool {
    data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures() {
        assert!(is_pdf(b"%PDF-1.7"));
        assert!(is_png(b"\x89PNG\r\n\x1a\nrest"));
        assert!(is_jpeg(b"\xff\xd8\xff\xe0"));
        assert!(is_gif(b"GIF89a") && is_gif(b"GIF87a"));
        assert!(is_webp(b"RIFF\0\0\0\0WEBPVP8 "));
        assert!(!is_webp(b"RIFF\0\0\0\0WEB"));
        for data in [b"".as_slice(), b"%PDF", b"\x89PNG", b"\xff\xd8", b"GIF88a"] {
            assert!(!is_pdf(data) && !is_png(data) && !is_jpeg(data) && !is_gif(data));
        }
    }
}
