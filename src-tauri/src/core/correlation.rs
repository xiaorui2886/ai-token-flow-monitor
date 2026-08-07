use crate::core::types::{
    CorrelationConfidence, CorrelationResult, RawSourceSample, RequestCorrelationKey,
};

pub struct RequestCorrelator;

impl RequestCorrelator {
    pub fn correlate(sample: &RawSourceSample) -> CorrelationResult {
        if let Some(req_id) = &sample.request_id {
            if !req_id.is_empty() {
                return CorrelationResult {
                    canonical_request_key: RequestCorrelationKey {
                        agent_id: sample.agent_id.clone(),
                        session_id: sample.session_id.clone(),
                        request_id: req_id.clone(),
                    },
                    correlation_method: "explicit_request_id".to_string(),
                    correlation_confidence: CorrelationConfidence::Exact,
                };
            }
        }

        if let Some(turn_id) = &sample.turn_id {
            if !turn_id.is_empty() {
                return CorrelationResult {
                    canonical_request_key: RequestCorrelationKey {
                        agent_id: sample.agent_id.clone(),
                        session_id: sample.session_id.clone(),
                        request_id: format!("turn_{}", turn_id),
                    },
                    correlation_method: "turn_id".to_string(),
                    correlation_confidence: CorrelationConfidence::Strong,
                };
            }
        }

        if let Some(resp_id) = &sample.response_id {
            if !resp_id.is_empty() {
                return CorrelationResult {
                    canonical_request_key: RequestCorrelationKey {
                        agent_id: sample.agent_id.clone(),
                        session_id: sample.session_id.clone(),
                        request_id: format!("resp_{}", resp_id),
                    },
                    correlation_method: "response_id".to_string(),
                    correlation_confidence: CorrelationConfidence::Strong,
                };
            }
        }

        if let Some(msg_id) = &sample.native_identity.native_message_id {
            if !msg_id.is_empty() {
                return CorrelationResult {
                    canonical_request_key: RequestCorrelationKey {
                        agent_id: sample.agent_id.clone(),
                        session_id: sample.session_id.clone(),
                        request_id: format!("msg_{}", msg_id),
                    },
                    correlation_method: "native_message_id".to_string(),
                    correlation_confidence: CorrelationConfidence::Strong,
                };
            }
        }

        if let (Some(f_hash), Some(offset)) = (
            &sample.native_identity.file_path_hash,
            sample.native_identity.byte_offset,
        ) {
            return CorrelationResult {
                canonical_request_key: RequestCorrelationKey {
                    agent_id: sample.agent_id.clone(),
                    session_id: sample.session_id.clone(),
                    request_id: format!("file_{}_{}", f_hash, offset),
                },
                correlation_method: "file_offset".to_string(),
                correlation_confidence: CorrelationConfidence::Weak,
            };
        }

        let fallback_id = format!("sample_{}", sample.sample_id);
        CorrelationResult {
            canonical_request_key: RequestCorrelationKey {
                agent_id: sample.agent_id.clone(),
                session_id: sample.session_id.clone(),
                request_id: fallback_id,
            },
            correlation_method: "fallback_sample_id".to_string(),
            correlation_confidence: CorrelationConfidence::Unknown,
        }
    }
}
