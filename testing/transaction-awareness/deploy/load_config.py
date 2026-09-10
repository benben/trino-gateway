"""Validate external benchmark database settings and create bounded lab patches."""

import copy
import ipaddress
from urllib.parse import parse_qs, urlsplit

from render import TASK, validate_priority_class


def prepare(config, database, addresses, replicas, *, priority_class=None):
    priority_name = validate_priority_class(priority_class)
    if type(replicas) is not int or not 2 <= replicas <= 100:
        raise ValueError("Benchmark replicas must be between two and one hundred")
    if set(database) != {"jdbcUrl", "user", "password", "driver"}:
        raise ValueError("Database input must contain only the four explicit connection fields")
    if database["driver"] != "org.postgresql.Driver" or not all(isinstance(value, str) and value for value in database.values()):
        raise ValueError("Benchmark requires explicit PostgreSQL credentials")
    if not database["jdbcUrl"].startswith("jdbc:postgresql://"):
        raise ValueError("Benchmark database must use PostgreSQL")
    uri = urlsplit(database["jdbcUrl"][5:])
    options = parse_qs(uri.query, keep_blank_values=True)
    allowed_options = {"sslmode", "sslrootcert", "currentSchema", "ApplicationName"}
    if set(options) - allowed_options or any(len(values) != 1 for values in options.values()) or uri.fragment:
        raise ValueError("Custom JDBC socket factories, hostname verifiers, and ambiguous options are not allowed")
    if options.get("sslmode") != ["verify-full"] or options.get("sslrootcert") != ["/etc/database-ca/ca.pem"]:
        raise ValueError("External database TLS must verify the hostname and mounted CA")
    if not uri.hostname or uri.username or uri.password or not uri.path.strip("/"):
        raise ValueError("Database URL must name a database without embedded credentials")
    if not addresses:
        raise ValueError("Explicit private database addresses are required")
    networks = []
    for value in addresses:
        address = ipaddress.ip_address(value)
        if not address.is_private or address.is_loopback or address.is_unspecified or address.is_link_local or address.is_multicast:
            raise ValueError("Database egress must identify private addresses")
        networks.append(str(address) + ("/32" if address.version == 4 else "/128"))
    updated = copy.deepcopy(config)
    updated.setdefault("dataStore", {}).update(database)
    network = {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
               "metadata": {"name": "benchmark-database", "labels": {"task": TASK}},
               "spec": {"podSelector": {"matchLabels": {"task": TASK, "app": "gateway"}},
                        "policyTypes": ["Egress"], "egress": [{"to": [{"ipBlock": {"cidr": value}} for value in networks],
                                                               "ports": [{"protocol": "TCP", "port": uri.port or 5432}]}]}}
    deployment = {"spec": {"replicas": replicas, "template": {"spec": {
        "preemptionPolicy": "Never", "priorityClassName": priority_name,
        "volumes": [{"name": "database-ca", "configMap": {"name": "benchmark-database-ca"}}],
        "containers": [{"name": "gateway", "volumeMounts": [{"name": "database-ca", "mountPath": "/etc/database-ca", "readOnly": True}]}]}}}}
    quota = {"spec": {"hard": {"requests.cpu": "60", "limits.cpu": "60", "requests.memory": "140Gi",
                                 "limits.memory": "140Gi", "pods": "120"}}}
    return updated, network, deployment, quota
