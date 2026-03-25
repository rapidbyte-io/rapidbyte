use serde::Deserialize;

/// Query parameters for paginated list endpoints.
/// Default limit is 20, maximum is 100.
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl PaginationParams {
    pub fn limit(&self) -> u32 {
        self.limit.unwrap_or(20).min(100).max(1)
    }

    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limit_is_20() {
        let p = PaginationParams {
            limit: None,
            cursor: None,
        };
        assert_eq!(p.limit(), 20);
    }

    #[test]
    fn limit_capped_at_100() {
        let p = PaginationParams {
            limit: Some(500),
            cursor: None,
        };
        assert_eq!(p.limit(), 100);
    }

    #[test]
    fn limit_at_least_1() {
        let p = PaginationParams {
            limit: Some(0),
            cursor: None,
        };
        assert_eq!(p.limit(), 1);
    }
}
