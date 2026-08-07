use crate::core::types::{NormalizedUsage, RawUsage, UsageAccountingStrategy, UsageSemantics};

pub struct UsageNormalizer;

impl UsageNormalizer {
    pub fn normalize(raw: &RawUsage, semantics: &UsageSemantics) -> NormalizedUsage {
        let raw_in = raw.raw_input_tokens.unwrap_or(0);
        let raw_out = raw.raw_output_tokens.unwrap_or(0);
        let cache_read = raw.raw_cache_read_tokens.unwrap_or(0);
        let cache_write = raw.raw_cache_write_tokens.unwrap_or(0);
        let reasoning = raw.raw_reasoning_tokens.unwrap_or(0);

        let (fresh_in, context_in) = match semantics.accounting_strategy {
            UsageAccountingStrategy::OpenAiStyle => {
                let fresh = raw_in.saturating_sub(cache_read);
                (fresh, raw_in)
            }
            UsageAccountingStrategy::AnthropicStyle => {
                let fresh = raw_in;
                let context = raw_in + cache_read + cache_write;
                (fresh, context)
            }
            UsageAccountingStrategy::GenericStyle => {
                let fresh = raw_in.saturating_sub(cache_read);
                (fresh, raw_in)
            }
        };

        let norm_output = if semantics.reasoning_is_output_subset {
            raw_out
        } else {
            raw_out + reasoning
        };

        let norm_total = context_in + norm_output;

        NormalizedUsage {
            normalized_context_input_tokens: context_in,
            normalized_fresh_input_tokens: fresh_in,
            normalized_output_tokens: norm_output,
            cache_read_tokens: cache_read,
            cache_write_tokens: cache_write,
            reasoning_tokens: reasoning,
            provider_reported_total: raw.raw_total_tokens,
            normalized_total: norm_total,
            usage_semantics: semantics.clone(),
            normalization_version: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_y1_openai_cached_input() {
        let semantics = UsageSemantics {
            reasoning_is_output_subset: true,
            accounting_strategy: UsageAccountingStrategy::OpenAiStyle,
            provider_name: "openai".to_string(),
        };
        let raw = RawUsage {
            raw_input_tokens: Some(1000),
            raw_output_tokens: Some(100),
            raw_cache_read_tokens: Some(600),
            raw_cache_write_tokens: Some(0),
            raw_reasoning_tokens: Some(0),
            raw_total_tokens: Some(1100),
        };
        let norm = UsageNormalizer::normalize(&raw, &semantics);

        assert_eq!(norm.normalized_context_input_tokens, 1000);
        assert_eq!(norm.normalized_fresh_input_tokens, 400);
    }

    #[test]
    fn test_y2_anthropic_cache() {
        let semantics = UsageSemantics {
            reasoning_is_output_subset: true,
            accounting_strategy: UsageAccountingStrategy::AnthropicStyle,
            provider_name: "anthropic".to_string(),
        };
        let raw = RawUsage {
            raw_input_tokens: Some(50),
            raw_output_tokens: Some(100),
            raw_cache_read_tokens: Some(100000),
            raw_cache_write_tokens: Some(2000),
            raw_reasoning_tokens: Some(0),
            raw_total_tokens: Some(102150),
        };
        let norm = UsageNormalizer::normalize(&raw, &semantics);

        assert_eq!(norm.normalized_context_input_tokens, 102050);
        assert_eq!(norm.normalized_fresh_input_tokens, 50);
    }
}
