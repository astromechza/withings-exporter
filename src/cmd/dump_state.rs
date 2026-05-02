use anyhow::Result;
use std::path::Path;

pub fn run(path: &Path) -> Result<()> {
    let mut state = crate::state::load(path)?;
    state.tokens.access_token = redact(&state.tokens.access_token);
    state.tokens.refresh_token = redact(&state.tokens.refresh_token);
    println!("{}", serde_json::to_string_pretty(&state)?);
    Ok(())
}

fn redact(s: &str) -> String {
    if s.len() < 8 {
        return "***".into();
    }
    format!("{}…{} ({} chars)", &s[..4], &s[s.len() - 4..], s.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redacts_long_string() {
        let r = redact("abcdefghijklmnop");
        assert!(r.starts_with("abcd"));
        assert!(r.contains("mnop"));
        assert!(r.contains("16 chars"));
    }
    #[test]
    fn redacts_short_string() {
        assert_eq!(redact("ab"), "***");
    }
}
