"""
Builds a Cassandra Session from the merged config dict.

Supports three connection modes, selected automatically based on config:

  1. AstraDB via Secure Connect Bundle (SCB)
       Set `secure_connect_bundle` to the path of your SCB .zip file and
       `astra_token` to your AstraCS:... application token.
       The SCB already contains all host, port, and SSL information — do NOT
       set `hosts`, `port`, or `ssl` alongside it.

  2. Standard Cassandra with username/password auth
       Set `hosts`, optional `port` (default 9042), `username`, `password`.

  3. Standard Cassandra with no auth (dev/local)
       Set `hosts` and optional `port` only.

SSL (modes 2 & 3 only):
  ssl: true
  ssl_cafile:   /path/to/ca.crt       # optional: verify server cert
  ssl_certfile: /path/to/client.crt   # optional: mutual TLS
  ssl_keyfile:  /path/to/client.key

Note: cassachange never creates keyspaces. All keyspaces must exist before
running cassachange (provisioned via Terraform or equivalent).
"""

import logging
import ssl as _ssl

log = logging.getLogger("cassachange")


def build_session(cfg: dict):
    try:
        from cassandra.cluster import Cluster
        from cassandra.auth import PlainTextAuthProvider
    except ImportError:
        raise RuntimeError(
            "cassandra-driver is not installed. "
            "Run: pip install cassandra-driver"
        )

    scb   = cfg.get("secure_connect_bundle")
    token = cfg.get("astra_token")

    # ------------------------------------------------------------------
    # Mode 1: AstraDB via Secure Connect Bundle
    # ------------------------------------------------------------------
    if scb:
        if not token:
            raise ValueError(
                "secure_connect_bundle is set but astra_token is missing. "
                "Provide your AstraCS:... token via `astra_token` in the "
                "config, the ASTRA_TOKEN environment variable, or --astra-token."
            )

        # The SCB already embeds host, port, and mTLS certificates.
        # Setting ssl_context or contact_points alongside cloud= raises an
        # exception in the driver, so we intentionally omit them here.
        # protocol_version=4 avoids noisy downgrade warnings -- AstraDB speaks v4.
        log.debug("Connecting to AstraDB via Secure Connect Bundle: %s", scb)
        auth_provider = PlainTextAuthProvider(
            username="token",   # literal string required by AstraDB
            password=token,
        )
        cluster = Cluster(
            cloud={"secure_connect_bundle": scb},
            auth_provider=auth_provider,
            protocol_version=4,
        )

    # ------------------------------------------------------------------
    # Mode 2 / 3: Standard Cassandra (with or without auth)
    # ------------------------------------------------------------------
    else:
        hosts = cfg.get("hosts", ["127.0.0.1"])
        port  = int(cfg.get("port", 9042))

        kwargs = dict(port=port)

        username = cfg.get("username")
        password = cfg.get("password")
        if username and password:
            kwargs["auth_provider"] = PlainTextAuthProvider(
                username=username, password=password
            )

        if cfg.get("ssl"):
            ssl_ctx = _ssl.create_default_context()
            if cfg.get("ssl_cafile"):
                ssl_ctx.load_verify_locations(cfg["ssl_cafile"])
            if cfg.get("ssl_certfile") and cfg.get("ssl_keyfile"):
                ssl_ctx.load_cert_chain(cfg["ssl_certfile"], cfg["ssl_keyfile"])
            kwargs["ssl_context"] = ssl_ctx

        log.debug("Connecting to Cassandra at %s:%s", hosts, port)
        cluster = Cluster(hosts, **kwargs)

    session = cluster.connect()
    log.debug("Connected.")
    return session
