from dataclasses import dataclass
import os
from typing import Any, Callable, List, Optional, TypeVar

T = TypeVar("T")
_MISSING = object()

FNV32_OFFSET, FNV32_PRIME = 0x811C9DC5, 0x01000193


def fnv1a_32(data: bytes) -> int:
    h = FNV32_OFFSET
    for byte in data:
        h ^= byte
        h = (h * FNV32_PRIME) & 0xFFFFFFFF
    return h


def get_env(name: str, cast: Callable[[str], T] = str, default: Any = _MISSING) -> T:
    """
    Lee una variable de entorno y la convierte con `cast`.
    Si no existe y no se especificó `default`, lanza KeyError.
    """
    if name not in os.environ:
        if default is not _MISSING:
            return default
        raise KeyError(f"Missing required environment variable: {name}")
    return cast(os.environ[name])


def get_env_list(name: str, sep: str = ",", default: Any = _MISSING) -> List[str]:
    """
    Lee una variable de entorno separada por `sep` y devuelve una lista de strings limpios.
    Si no existe y no se especificó `default`, lanza KeyError.
    """
    raw = get_env(name, str, default=default)
    if isinstance(raw, list):
        return raw
    return [item.strip() for item in raw.split(sep) if item.strip()]


@dataclass(frozen=True)
class ServiceInfo:
    rabbitmq_host: str
    replica_id: Any
    service_name: Optional[str] = None


@dataclass(frozen=True)
class HeartbeatConfig:
    exchange_name: str
    interval: float
    service_name: Optional[str] = None


@dataclass(frozen=True)
class ControlInfo:
    control_exchange_name: Optional[str]
    control_routing_key: str
    previous_stage_replicas: int
    next_stage_replicas: int


@dataclass(frozen=True)
class WorkerInfo:
    service: ServiceInfo
    control: ControlInfo
    batch_size: int
    input_queue_name: Optional[str] = None


@dataclass(frozen=True)
class WatchdogConfig:
    service: ServiceInfo
    heartbeat: HeartbeatConfig
    heartbeat_timeout: float
    initial_grace_period: float
    revive_grace_period: float
    watchlist: List[str]
    total_watchdogs: int
    election_timeout: float
    watchdog_exchange_name: str


def get_service_info(
    replica_id_cast: Callable[[str], Any] = str,
    require_service_name: bool = False,
) -> ServiceInfo:
    """Lee la identidad y conexión base de un nodo (worker o watchdog)."""
    service_name = (
        get_env("SERVICE_NAME")
        if require_service_name
        else get_env("SERVICE_NAME", default=None)
    )
    return ServiceInfo(
        rabbitmq_host=get_env("RABBITMQ_HOST"),
        replica_id=get_env("REPLICA_ID", replica_id_cast),
        service_name=service_name,
    )


def get_heartbeat_config(
    required: bool = True,
    require_service_name: bool = True,
) -> Optional[HeartbeatConfig]:
    """
    Lee la configuración de heartbeats (compartida por BaseWorker y Watchdog).
    Si `required=False` y `HEARTBEAT_EXCHANGE_NAME` no está definida, retorna None.
    """
    exchange_name = (
        get_env("HEARTBEAT_EXCHANGE_NAME")
        if required
        else get_env("HEARTBEAT_EXCHANGE_NAME", default=None)
    )
    if not exchange_name:
        return None

    service_name = (
        get_env("SERVICE_NAME")
        if require_service_name
        else get_env("SERVICE_NAME", default=None)
    )
    return HeartbeatConfig(
        exchange_name=exchange_name,
        interval=get_env("HEARTBEAT_INTERVAL", float),
        service_name=service_name,
    )


def get_control_info() -> ControlInfo:
    """Lee la configuración de control y topología de etapas de un worker."""
    return ControlInfo(
        control_exchange_name=get_env("CONTROL_EXCHANGE_NAME", default=None),
        control_routing_key=get_env("CONTROL_ROUTING_KEY", default=""),
        previous_stage_replicas=get_env("PREVIOUS_STAGE_REPLICAS", int, default=1),
        next_stage_replicas=get_env("NEXT_STAGE_REPLICAS", int, default=1),
    )


def get_worker_info(
    batch_size_default: int = 10,
    require_input_queue: bool = False,
) -> WorkerInfo:
    """Lee la configuración común de un worker (identidad, control, batch y cola de entrada)."""
    input_queue_name = (
        get_env("INPUT_QUEUE_NAME")
        if require_input_queue
        else get_env("INPUT_QUEUE_NAME", default=None)
    )
    return WorkerInfo(
        service=get_service_info(),
        control=get_control_info(),
        batch_size=get_env("BATCH_SIZE", int, default=batch_size_default),
        input_queue_name=input_queue_name,
    )


def get_watchdog_config() -> WatchdogConfig:
    """Lee toda la configuración requerida por el Watchdog (sin valores por defecto)."""
    return WatchdogConfig(
        service=get_service_info(replica_id_cast=int, require_service_name=False),
        heartbeat=get_heartbeat_config(required=True, require_service_name=False),
        heartbeat_timeout=get_env("HEARTBEAT_TIMEOUT", float),
        initial_grace_period=get_env("INITIAL_GRACE_PERIOD", float),
        revive_grace_period=get_env("REVIVE_GRACE_PERIOD", float),
        watchlist=get_env_list("WATCHLIST"),
        total_watchdogs=get_env("TOTAL_WATCHDOGS", int),
        election_timeout=get_env("ELECTION_TIMEOUT", float),
        watchdog_exchange_name=get_env("WATCHDOG_EXCHANGE_NAME"),
    )
