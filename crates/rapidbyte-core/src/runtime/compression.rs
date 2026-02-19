/// IPC channel compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Lz4,
    Zstd,
}

impl CompressionCodec {
    /// Parse from pipeline config string. Returns None for unknown/empty values.
    pub fn from_str_opt(s: Option<&str>) -> Option<Self> {
        match s {
            Some("lz4") => Some(Self::Lz4),
            Some("zstd") => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Compress bytes using the given codec.
pub fn compress(codec: CompressionCodec, data: &[u8]) -> Vec<u8> {
    match codec {
        CompressionCodec::Lz4 => lz4_flex::compress_prepend_size(data),
        CompressionCodec::Zstd => zstd::bulk::compress(data, 1).expect("zstd compress failed"),
    }
}

/// Decompress bytes using the given codec.
pub fn decompress(codec: CompressionCodec, data: &[u8]) -> Vec<u8> {
    match codec {
        CompressionCodec::Lz4 => {
            lz4_flex::decompress_size_prepended(data).expect("lz4 decompress failed")
        }
        CompressionCodec::Zstd => zstd::decode_all(data).expect("zstd decompress failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"hello world repeated hello world repeated hello world repeated";
        let compressed = compress(CompressionCodec::Lz4, data);
        let decompressed = decompress(CompressionCodec::Lz4, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"hello world repeated hello world repeated hello world repeated";
        let compressed = compress(CompressionCodec::Zstd, data);
        let decompressed = decompress(CompressionCodec::Zstd, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_lz4_compresses_repetitive_data() {
        let data = vec![42u8; 10_000];
        let compressed = compress(CompressionCodec::Lz4, &data);
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_zstd_compresses_repetitive_data() {
        let data = vec![42u8; 10_000];
        let compressed = compress(CompressionCodec::Zstd, &data);
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_codec_from_str() {
        assert_eq!(CompressionCodec::from_str_opt(Some("lz4")), Some(CompressionCodec::Lz4));
        assert_eq!(CompressionCodec::from_str_opt(Some("zstd")), Some(CompressionCodec::Zstd));
        assert_eq!(CompressionCodec::from_str_opt(Some("none")), None);
        assert_eq!(CompressionCodec::from_str_opt(None), None);
    }

    #[test]
    fn test_empty_data_roundtrip() {
        let data = b"";
        let compressed = compress(CompressionCodec::Lz4, data);
        let decompressed = decompress(CompressionCodec::Lz4, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());

        let compressed = compress(CompressionCodec::Zstd, data);
        let decompressed = decompress(CompressionCodec::Zstd, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }
}
