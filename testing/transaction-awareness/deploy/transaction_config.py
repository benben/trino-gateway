"""Preserve shared lab keys when toggling transaction-aware routing."""

import copy
import secrets


def configure_transactions(config, *, enabled, terminal_retention):
    if not isinstance(config, dict) or not config.get("dataStore", {}).get("jdbcUrl", "").startswith("jdbc:postgresql://"):
        raise ValueError("Transaction-aware lab configuration requires PostgreSQL")
    if type(terminal_retention) is not int or not 1 <= terminal_retention <= 86400:
        raise ValueError("Terminal retention must be an integer between 1 and 86400 seconds")
    result = copy.deepcopy(config)
    settings = result.setdefault("transactionAwareness", {})
    if not isinstance(settings, dict):
        raise ValueError("Existing transaction settings are invalid")
    for key in ["identityKey", "adminToken"]:
        if key not in settings:
            settings[key] = secrets.token_hex(32)
        if not isinstance(settings[key], str) or len(settings[key].encode()) < 32:
            raise ValueError("Existing transaction keys must not be silently replaced")
    if settings["identityKey"] == settings["adminToken"]:
        raise ValueError("Transaction identity and administration keys must differ")
    settings.update(enabled=enabled, terminalRetentionSeconds=terminal_retention)
    return result
