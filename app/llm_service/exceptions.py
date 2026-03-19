from typing import Optional, List

class LLMError(Exception):
    """Base exception for LLM errors."""
    pass

class LLMRateLimitError(LLMError):
    """Specific error for 429 Rate Limit."""
    def __init__(
        self,
        message: str,
        recommendations: Optional[List[str]] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None
    ):
        super().__init__(message)
        self.recommendations = recommendations or []
        self.provider = provider
        self.model = model

def is_rate_limit(error_obj) -> bool:
    """Helper to detect rate limit keywords in various error types."""
    err_str = str(error_obj).lower()
    return "429" in err_str or "rate limit" in err_str or "too many requests" in err_str
