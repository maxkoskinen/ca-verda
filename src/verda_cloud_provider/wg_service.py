from __future__ import annotations

import ipaddress
import logging
import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from verda_cloud_provider.settings import (
    LocalWireguardBackendConfig,
    SSHWireguardBackendConfig,
    WireguardBackendConfig,
    WireguardConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class WireguardPeerConfig:
    """Returned by reserve() — everything needed to render the startup script."""
    reservation_id: str   # internal handle — pass to commit() or release()
    tunnel_ip: str
    private_key: str
    public_key: str
    peer_pubkey: str
    wg_listen_port: str
    allowed_ips: list[str] = field(default_factory=list)


@dataclass
class _Reservation:
    reservation_id: str
    tunnel_ip: str
    public_key: str   # node pubkey — written to bastion on commit()


class WireguardBackend(Protocol):
    def run(self, cmd: list[str], input: str | None = None) -> str: ...
    def read_file(self, path: str) -> str: ...
    def write_file(self, path: str, content: str) -> None: ...
    def exists(self, path: str) -> bool: ...


class LocalBackend:
    def run(self, cmd: list[str], input: str | None = None) -> str:
        r = subprocess.run(
            cmd,
            input=input,
            capture_output=True,
            text=True,
            check=True,
        )
        return r.stdout

    def read_file(self, path: str) -> str:
        return Path(path).read_text()

    def write_file(self, path: str, content: str) -> None:
        Path(path).write_text(content)

    def exists(self, path: str) -> bool:
        return Path(path).exists()

class SSHBackend:
    def __init__(self, host: str, port: int, user: str, key_path: str):
        self.host = host
        self.port = port
        self.user = user
        self.key_path = key_path

    def _ssh(self, remote_cmd: str, input: str | None = None) -> str:
        cmd = [
            "ssh",
            "-i", self.key_path,
            "-p", str(self.port),
            f"{self.user}@{self.host}",
            remote_cmd,
        ]
        r = subprocess.run(
            cmd,
            input=input,
            capture_output=True,
            text=True,
            check=True,
        )
        return r.stdout

    def run(self, cmd: list[str], input: str | None = None) -> str:
        return self._ssh(" ".join(cmd), input=input)

    def read_file(self, path: str) -> str:
        return self._ssh(f"cat {path}")

    def write_file(self, path: str, content: str) -> None:
        self._ssh(f"tee {path}", input=content)

    def exists(self, path: str) -> bool:
        try:
            self._ssh(f"test -f {path}")
            return True
        except subprocess.CalledProcessError:
            return False


class WireguardService:
    """
    Two-phase WireGuard peer lifecycle:

      reserve()  — pick the next free IP, generate a keypair, hold the IP
                   in memory. Returns WireguardPeerConfig for the startup
                   script template. Nothing is written to the bastion yet.

      commit(reservation_id, instance_id)
                 — register the peer on the live wg interface and persist.
                   Call after Verda instance creation succeeds.

      release(reservation_id)
                 — discard the reservation without touching the bastion.
                   Call in the failure path if instance creation fails.

      remove_peer(instance_id)
                 — remove a committed peer.
    """

    def __init__(self, config: WireguardConfig) -> None:
        self.config = config
        net = ipaddress.IPv4Network(config.tunnel_network, strict=True)
        self._usable: list[str] = [str(h) for h in list(net.hosts())[1:]]  # skip .1
        self._peer_pubkey: str | None = config.server_pub_key
        self._pending: dict[str, _Reservation] = {}
        self.backend = self.build_wireguard_backend(config.backend)

    def build_wireguard_backend(self, cfg: WireguardBackendConfig):
        if isinstance(cfg, LocalWireguardBackendConfig):
            return LocalBackend()

        if isinstance(cfg, SSHWireguardBackendConfig):
            return SSHBackend(
                host=cfg.host,
                user=cfg.user,
                key_path=cfg.private_key_path,
                port=cfg.port,
            )
        raise RuntimeError("Unsupported backend type")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def reserve(self) -> WireguardPeerConfig:
        """
        Phase 1: allocate a tunnel IP + keypair and hold them.
        Nothing is written to the bastion wg interface.
        """
        tunnel_ip = self._next_free_ip()
        privkey, pubkey = self._generate_keypair()
        reservation_id = str(uuid.uuid4())

        self._pending[reservation_id] = _Reservation(
            reservation_id=reservation_id,
            tunnel_ip=tunnel_ip,
            public_key=pubkey,
        )
        logger.debug("WireGuard IP reserved: %s (reservation=%s)", tunnel_ip, reservation_id)

        return WireguardPeerConfig(
            reservation_id=reservation_id,
            tunnel_ip=tunnel_ip,
            private_key=privkey,
            public_key=pubkey,
            peer_pubkey=self._get_peer_pubkey(),
            allowed_ips=self.config.cloud_allowed_ips + [self.config.tunnel_network],
            wg_listen_port=str(self.config.listen_port)
        )

    def commit(self, reservation_id: str, instance_id: str, node_endpoint: str | None = None) -> None:
        reservation = self._pending.pop(reservation_id, None)
        if reservation is None:
            raise KeyError(f"No pending reservation: {reservation_id!r}")
        try:
            self._add_peer(
                pubkey=reservation.public_key,
                tunnel_ip=reservation.tunnel_ip,
                instance_id=instance_id,
                node_endpoint=node_endpoint,
            )
        except Exception as e:
            logger.warning(f"committing failed with error: {e}")
            self._pending[reservation_id] = reservation
            return
        logger.info("WireGuard peer committed: instance=%s ip=%s", instance_id, reservation.tunnel_ip)

    def release(self, reservation_id: str) -> None:
        """
        Discard a pending reservation without touching the bastion.
        Call when instance creation fails.
        """
        reservation = self._pending.pop(reservation_id, None)
        if reservation:
            logger.info(
                "WireGuard reservation released: ip=%s reservation=%s",
                reservation.tunnel_ip, reservation_id,
            )
        else:
            logger.warning("release: unknown reservation_id=%s", reservation_id)

    def remove_peer(self, instance_id: str) -> None:
        """Remove a committed peer by its instance_id."""
        pubkey = self._find_pubkey_for_instance(instance_id)
        if pubkey is None:
            logger.warning("remove_peer: no peer found for instance %s", instance_id)
            return
        self.backend.run(["wg", "set", self.config.interface, "peer", pubkey, "remove"])
        self._save()
        logger.info("WireGuard peer removed: instance=%s", instance_id)

    # ------------------------------------------------------------------
    # IP allocation — live wg dump + in-flight reservations
    # ------------------------------------------------------------------

    def _used_ips(self) -> set[str]:
        # In-flight reservations block their IPs immediately
        used: set[str] = {r.tunnel_ip for r in self._pending.values()}
        try:
            out = self.backend.run(["wg", "show", self.config.interface, "dump"])
        except subprocess.CalledProcessError:
            return used
        for line in out.splitlines()[1:]:
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            for cidr in parts[3].split(","):
                cidr = cidr.strip()
                if cidr.endswith("/32"):
                    used.add(cidr[:-3])
        return used

    def _next_free_ip(self) -> str:
        used = self._used_ips()
        for ip in self._usable:
            if ip not in used:
                return ip
        raise RuntimeError(
            f"WireGuard pool {self.config.tunnel_network} exhausted "
            f"({len(used)} IPs in use or reserved)"
        )

    # ------------------------------------------------------------------
    # wg0.conf instance index
    # ------------------------------------------------------------------

    def _conf_path(self) -> str:
        return f"/etc/wireguard/{self.config.interface}.conf"

    def _peer_map(self) -> dict[str, str]:
        path = self._conf_path()

        if not self.backend.exists(path):
            return {}

        content = self.backend.read_file(path)

        result: dict[str, str] = {}
        current_instance: str | None = None

        for line in content.splitlines():
            line = line.strip()
            if line.startswith("# instance:"):
                current_instance = line.split(":", 1)[1].strip()
            elif line.startswith("PublicKey") and current_instance:
                result[current_instance] = line.split("=", 1)[1].strip()
                current_instance = None

        return result

    def _find_pubkey_for_instance(self, instance_id: str) -> str | None:
        return self._peer_map().get(instance_id)

    # ------------------------------------------------------------------
    # wg subprocess helpers
    # ------------------------------------------------------------------

    def _generate_keypair(self) -> tuple[str, str]:
        priv = self.backend.run(["wg", "genkey"]).strip()
        pub  = self.backend.run(["wg", "pubkey"], input=priv).strip()
        return priv, pub

    def _get_peer_pubkey(self) -> str:
        if self._peer_pubkey is None:
            priv = self.backend.read_file(self.config.server_privkey_path).strip()
            self._peer_pubkey = self.backend.run(["wg", "pubkey"], input=priv).strip()
        return self._peer_pubkey

    def _add_peer(self, pubkey: str, tunnel_ip: str, instance_id: str, node_endpoint: str | None = None) -> None:
        allowed = f"{tunnel_ip}/32,10.244.0.0/16,10.96.0.0/12,192.168.49.0/24"
        cmd = [
            "wg", "set", self.config.interface,
            "peer", pubkey,
            "allowed-ips", allowed,
        ]
        if node_endpoint:
            cmd += ["endpoint", node_endpoint]
            cmd += ["persistent-keepalive", str(5)]

        self.backend.run(cmd)

        self._save()
        self._annotate_conf(pubkey=pubkey, instance_id=instance_id)

    def _save(self) -> None:
        self.backend.run(["wg-quick", "save", self.config.interface])

    def _annotate_conf(self, pubkey: str, instance_id: str) -> None:
        path = self._conf_path()

        content = self.backend.read_file(path)
        lines = content.splitlines(keepends=True)

        out: list[str] = []
        i = 0

        while i < len(lines):
            line = lines[i]
            if line.strip() == "[Peer]":
                block, j = [line], i + 1
                while j < len(lines) and lines[j].strip() not in ("[Peer]", "[Interface]"):
                    block.append(lines[j])
                    j += 1

                if pubkey in "".join(block):
                    prev = out[-1].strip() if out else ""
                    if not prev.startswith("# instance:"):
                        out.append(f"# instance:{instance_id}\n")

                out.extend(block)
                i = j
            else:
                out.append(line)
                i += 1

        self.backend.write_file(path, "".join(out))
