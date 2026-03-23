use serde::Deserialize;

/// Query parameters for paginated list endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl PaginationParams {
    /// Returns the effective limit, clamped to a maximum of 100.
    /// Defaults to 20 if not provided.
    #[must_use]
    pub fn limit(&self) -> u32 {
        self.limit.unwrap_or(20).min(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limit_is_20() {
        let params = PaginationParams {
            limit: None,
            cursor: None,
        };
        assert_eq!(params.limit(), 20);
    }

    #[test]
    fn explicit_limit_is_respected() {
        let params = PaginationParams {
            limit: Some(50),
            cursor: None,
        };
        assert_eq!(params.limit(), 50);
    }

    #[test]
    fn limit_is_clamped_to_100() {
        let params = PaginationParams {
            limit: Some(200),
            cursor: None,
        };
        assert_eq!(params.limit(), 100);
    }

    #[test]
    fn zero_limit_is_zero() {
        let params = PaginationParams {
            limit: Some(0),
            cursor: None,
        };
        assert_eq!(params.limit(), 0);
    }
}
