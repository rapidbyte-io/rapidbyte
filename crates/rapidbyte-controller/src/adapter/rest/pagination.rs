use serde::Deserialize;

/// Clamp a pagination limit: default 20, min 1, max 100.
pub fn clamp_page_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(20).min(100).max(1)
}

/// Split a comma-separated tag string into a Vec.
pub fn split_tags(tags: Option<String>) -> Option<Vec<String>> {
    tags.map(|t| {
        t.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    })
}

/// Query parameters for paginated list endpoints.
/// Default limit is 20, maximum is 100.
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl PaginationParams {
    pub fn limit(&self) -> u32 {
        clamp_page_limit(self.limit)
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

    #[test]
    fn clamp_page_limit_none_is_20() {
        assert_eq!(clamp_page_limit(None), 20);
    }

    #[test]
    fn split_tags_none_is_none() {
        assert_eq!(split_tags(None), None);
    }

    #[test]
    fn split_tags_trims_and_filters() {
        let result = split_tags(Some("a, b, , c".to_string()));
        assert_eq!(
            result,
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }
}
