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
