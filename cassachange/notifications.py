"""
cassachange.notifications — Webhook / Slack / Teams notifications.

Configure in cassachange.yml (or in a profile block):

  notifications:
    on_events:                # filter — omit to fire on every event
      - deploy_success
      - deploy_failed
      - rollback_success
      - rollback_failed
      - script_failed

    channels:
      - type: slack
        webhook_url_env: SLACK_WEBHOOK_URL     # read URL from env var
        # OR inline:  webhook_url: https://hooks.slack.com/services/...

      - type: teams
        webhook_url_env: TEAMS_WEBHOOK_URL

      - type: webhook                          # generic HTTP webhook
        url: https://ops.example.com/hooks/cassachange
        method: POST
        timeout: 10
        headers:
          Authorization: "Bearer ${NOTIFY_TOKEN}"   # ${ENV} substitution

Event types emitted by cassachange:
  deploy_start   deploy_success   deploy_failed
  rollback_start rollback_success rollback_failed
  script_failed  repair

Notifications are fire-and-forget. A failed delivery is logged as a
WARNING and never blocks or fails the migration.
"""

import datetime
import json
import logging
import os
import re
import socket
import urllib.request
import urllib.error

log = logging.getLogger("cassachange.notifications")


def notify(cfg: dict, event: str, **kwargs):
    """
    Fire all configured notification channels for *event*.
    Extra keyword args (keyspace, script, detail, status, tag) are
    included verbatim in the payload.
    """
    block = cfg.get("notifications")
    if not block:
        return

    on_events = block.get("on_events") or []
    if on_events and event not in on_events:
        return

    channels = block.get("channels") or []
    if not channels:
        return

    payload = {
        "event":       event,
        "run_id":      cfg.get("_run_id", ""),
        "profile":     cfg.get("profile", ""),
        "environment": cfg.get("environment", "") or os.getenv("CASSACHANGE_ENV", ""),
        "tag":         cfg.get("tag", ""),
        "keyspace":    kwargs.get("keyspace", ""),
        "script":      kwargs.get("script", ""),
        "status":      kwargs.get("status", ""),
        "detail":      kwargs.get("detail", ""),
        "hostname":    socket.gethostname(),
        "timestamp":   datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    for ch in channels:
        try:
            _dispatch(ch, payload, event)
        except Exception as exc:
            log.warning("Notification failed [%s]: %s", ch.get("type", "?"), exc)


# ---------------------------------------------------------------------------
# Channel dispatch
# ---------------------------------------------------------------------------

def _dispatch(channel: dict, payload: dict, event: str):
    ch_type = (channel.get("type") or "").lower()
    if ch_type == "slack":
        _slack(channel, payload, event)
    elif ch_type == "teams":
        _teams(channel, payload, event)
    elif ch_type == "webhook":
        _generic(channel, payload)
    else:
        log.warning("Unknown notification channel type '%s'", ch_type)


# ---------------------------------------------------------------------------
# Slack (Block Kit)
# ---------------------------------------------------------------------------

def _slack(channel: dict, payload: dict, event: str):
    url    = _webhook_url(channel)
    failed = "failed" in event
    icon   = ":x:" if failed else ":white_check_mark:"

    fields = []
    for label, key in [("Profile", "profile"), ("Env", "environment"),
                       ("Keyspace", "keyspace"), ("Tag", "tag"),
                       ("Script", "script")]:
        if payload.get(key):
            fields.append({"type": "mrkdwn", "text": f"*{label}:*\n{payload[key]}"})
    if payload.get("run_id"):
        fields.append({"type": "mrkdwn", "text": f"*Run ID:*\n{payload['run_id'][:8]}"})

    blocks: list = [{
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": f"{icon} *cassachange — {event.replace('_', ' ').title()}*"}
    }]
    if payload.get("detail"):
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": payload["detail"]}})
    if fields:
        blocks.append({"type": "section", "fields": fields[:10]})
    blocks.append({"type": "context", "elements": [
        {"type": "mrkdwn",
         "text": f"Host: {payload['hostname']} | {payload['timestamp']}"}
    ]})

    _post(url, {"blocks": blocks}, timeout=channel.get("timeout", 10))
    log.debug("Slack notification sent: %s", event)


# ---------------------------------------------------------------------------
# Microsoft Teams (Adaptive Card)
# ---------------------------------------------------------------------------

def _teams(channel: dict, payload: dict, event: str):
    url    = _webhook_url(channel)
    failed = "failed" in event
    color  = "attention" if failed else "good"

    facts = [{"title": k.title(), "value": payload[k]}
             for k in ("profile", "environment", "keyspace", "tag", "script", "hostname")
             if payload.get(k)]
    if payload.get("run_id"):
        facts.append({"title": "Run ID", "value": payload["run_id"][:8]})

    body: list = [{"type": "TextBlock",
                   "text": f"cassachange — {event.replace('_', ' ').title()}",
                   "weight": "bolder", "size": "medium", "color": color}]
    if payload.get("detail"):
        body.append({"type": "TextBlock", "text": payload["detail"], "wrap": True})
    if facts:
        body.append({"type": "FactSet", "facts": facts})

    card = {"type": "message", "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard", "version": "1.2", "body": body}
    }]}
    _post(url, card, timeout=channel.get("timeout", 10))
    log.debug("Teams notification sent: %s", event)


# ---------------------------------------------------------------------------
# Generic webhook
# ---------------------------------------------------------------------------

def _generic(channel: dict, payload: dict):
    url = channel.get("url", "")
    if not url:
        raise ValueError("Webhook channel missing 'url'")

    method  = (channel.get("method") or "POST").upper()
    timeout = channel.get("timeout", 10)

    # ${ENV_VAR} substitution in header values
    headers = {}
    for k, v in (channel.get("headers") or {}).items():
        resolved = str(v)
        for m in re.finditer(r"\$\{([^}]+)\}", resolved):
            resolved = resolved.replace(m.group(0), os.getenv(m.group(1), ""))
        headers[k] = resolved

    _post(url, payload, method=method, extra_headers=headers,
          timeout=timeout)
    log.debug("Webhook notification sent to %s", url)


# ---------------------------------------------------------------------------
# HTTP helper  (stdlib only — no requests dependency)
# ---------------------------------------------------------------------------

def _post(url: str, body: dict, method: str = "POST",
          extra_headers: dict = None, timeout: int = 10):
    data = json.dumps(body).encode()
    req  = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (extra_headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}")


def _webhook_url(channel: dict) -> str:
    url = channel.get("webhook_url", "")
    if not url and channel.get("webhook_url_env"):
        url = os.getenv(channel["webhook_url_env"], "")
    if not url:
        raise ValueError("Channel missing webhook_url / webhook_url_env")
    return url
