"""cassachange — Cassandra schema migration tool."""

__version__ = "1.0.0"
__edition__  = "community"


def _enterprise_registered() -> bool:
    """Return True if cassachange-enterprise is installed and registered."""
    try:
        from importlib.metadata import entry_points
        eps = entry_points(group="cassachange.enterprise")
        return bool(list(eps))
    except Exception:
        return False


def get_edition() -> str:
    return "enterprise" if _enterprise_registered() else "community"
