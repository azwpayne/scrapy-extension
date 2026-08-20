"""Shared reliability policy constants."""

# The public Settings model and the runtime pipeline factory must not grow
# independent failure-policy defaults.
DEFAULT_PIPELINE_MAX_STORAGE_ERRORS = 10

__all__ = ["DEFAULT_PIPELINE_MAX_STORAGE_ERRORS"]
