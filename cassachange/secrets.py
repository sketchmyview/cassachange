"""
cassachange.secrets — Community edition stub.

Secret manager integration (HashiCorp Vault, AWS SSM,
AWS Secrets Manager, Azure Key Vault) is an enterprise feature.

Upgrade:
  pip install cassachange-enterprise

Contact: enterprise@cassachange.com
"""

import logging
log = logging.getLogger("cassachange.secrets")


class SecretsError(Exception):
    pass


_ENTERPRISE_MSG = (
    "\n"
    "  Secret manager integration requires cassachange-enterprise.\n"
    "\n"
    "  Community edition:  pip install cassachange\n"
    "  Enterprise edition: pip install cassachange-enterprise\n"
    "\n"
    "  Contact: enterprise@cassachange.com\n"
)


def resolve_secrets(cfg: dict) -> dict:
    """
    Check if secrets block is present in config.
    If it is, the user needs cassachange-enterprise.
    If not, return cfg unchanged.
    """
    if cfg.get("secrets") or cfg.get("secrets_provider"):
        raise SecretsError(_ENTERPRISE_MSG)
    return cfg


def cleanup_temp_files() -> None:
    """No-op in community edition — no temp files are created."""
    pass
