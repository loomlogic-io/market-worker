import asyncio
import json
import logging
import os
import signal
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
import websockets
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Config:
    supabase_url: str
    supabase_service_key: str
    polygon_api_key: str

    worker_name: str
    worker_version: str
    environment: str
    provider_name: str
    ws_url: str

    heartbeat_interval_seconds: int
    snapshot_flush_interval_seconds: int
    subscription_refresh_interval_seconds: int
    stale_threshold_seconds: int
    write_batch_max_size: int

    enable_snapshot_writes: bool
    enable_trade_writes: bool
    enable_quote_subscriptions: bool

    alert_cooldown_seconds: int
    system_alert_user_id: Optional[str]

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            supabase_url=os.environ["SUPABASE_URL"],
            supabase_service_key=os.environ["SUPABASE_SERVICE_ROLE_KEY"],
            polygon_api_key=os.environ["POLYGON_API_KEY"],
            worker_name=os.getenv("WORKER_NAME", "market-worker-01"),
            worker_version=os.getenv("WORKER_VERSION", "1.0.0"),
            environment=os.getenv("ENVIRONMENT", "paper"),
            provider_name=os.getenv("PROVIDER_NAME", "polygon"),
            ws_url=os.getenv("WS_URL", "wss://socket.polygon.io/stocks"),
            heartbeat_interval_seconds=int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "10")),
            snapshot_flush_interval_seconds=int(os.getenv("SNAPSHOT_FLUSH_INTERVAL_SECONDS", "2")),
            subscription_refresh_interval_seconds=int(os.getenv("SUBSCRIPTION_REFRESH_INTERVAL_SECONDS", "30")),
            stale_threshold_seconds=int(os.getenv("STALE_THRESHOLD_SECONDS", "20")),
            write_batch_max_size=int(os.getenv("WRITE_BATCH_MAX_SIZE", "500")),
            enable_snapshot_writes=as_bool(os.getenv("ENABLE_SNAPSHOT_WRITES"), True),
            enable_trade_writes=as_bool(os.getenv("ENABLE_TRADE_WRITES"), False),
            enable_quote_subscriptions=as_bool(os.getenv("ENABLE_QUOTE_SUBSCRIPTIONS"), False),
            alert_cooldown_seconds=int(os.getenv("ALERT_COOLDOWN_SECONDS", "60")),
            system_alert_user_id=os.getenv("SYSTEM_ALERT_USER_ID") or None,
        )


class AlertRouter:
    def __init__(self, supabase: Client, config: Config, logger: logging.Logger):
        self.supabase = supabase
        self.config = config
        self.logger = logger
        self.last_alert_sent_at: Dict[str, float] = {}

    def _cooldown_key(self, user_id: str, alert_type: str, symbol: Optional[str]) -> str:
        return f"{user_id}:{alert_type}:{symbol or '-'}"

    def _should_send(self, user_id: str, alert_type: str, symbol: Optional[str]) -> bool:
        key = self._cooldown_key(user_id, alert_type, symbol)
        now = time.time()
        last = self.last_alert_sent_at.get(key, 0)
        if now - last >= self.config.alert_cooldown_seconds:
            self.last_alert_sent_at[key] = now
            return True
        return False

    def dispatch_alert(
        self,
        user_id: str,
        alert_type: str,
        title: str,
        message: str,
        metadata: Optional[dict] = None,
    ) -> Optional[dict]:
        if not self._should_send(user_id, alert_type, metadata.get("symbol") if metadata else None):
            return None

        try:
            resp = httpx.post(
                f"{self.config.supabase_url}/functions/v1/dispatch-alert",
                headers={
                    "Authorization": f"Bearer {self.config.supabase_service_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "user_id": user_id,
                    "alert_type": alert_type,
                    "title": title,
                    "message": message,
                    "metadata": metadata or {},
                },
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            self.logger.warning("dispatch-alert failed for user %s: %s", user_id, exc)
            return None

    def resolve_users_for_symbol(self, symbol: str) -> Set[str]:
        """
        User-aware alert routing:
        1. users tracking symbol in watchlists
        2. users with open positions on symbol
        3. users with active signals on symbol
        4. explicit notification subscriptions
        """
        users: Set[str] = set()

        optional_queries: List[Tuple[str, str, Dict[str, Any]]] = [
            (
                "market_watchlist_items",
                "symbol,user_id",
                {"symbol": symbol},
            ),
            (
                "positions",
                "symbol,user_id,status",
                {"symbol": symbol, "status": "open"},
            ),
            (
                "signals",
                "symbol,user_id,status",
                {"symbol": symbol, "status": "active"},
            ),
            (
                "user_notification_subscriptions",
                "user_id,symbol,is_enabled",
                {"symbol": symbol, "is_enabled": True},
            ),
        ]

        for table, select_cols, filters in optional_queries:
            try:
                query = self.supabase.table(table).select(select_cols)
                for key, value in filters.items():
                    query = query.eq(key, value)
                result = query.execute()
                for row in result.data or []:
                    user_id = row.get("user_id")
                    if user_id:
                        users.add(user_id)
            except Exception:
                # optional tables; ignore if absent or schema differs
                continue

        if not users and self.config.system_alert_user_id:
            users.add(self.config.system_alert_user_id)

        return users

    def send_symbol_alert(
        self,
        symbol: str,
        alert_type: str,
        title: str,
        message: str,
        metadata: Optional[dict] = None,
    ) -> None:
        users = self.resolve_users_for_symbol(symbol)
        if not users:
            return

        payload = metadata or {}
        payload.setdefault("symbol", symbol)
        payload.setdefault("env", self.config.environment)

        for user_id in users:
            self.dispatch_alert(
                user_id=user_id,
                alert_type=alert_type,
                title=title,
                message=message,
                metadata=payload,
            )

    def send_system_alert(self, title: str, message: str, metadata: Optional[dict] = None) -> None:
        if not self.config.system_alert_user_id:
            return
        self.dispatch_alert(
            user_id=self.config.system_alert_user_id,
            alert_type="system_error",
            title=title,
            message=message,
            metadata=metadata or {"env": self.config.environment},
        )


class MarketWorker:
    def __init__(self, config: Config):
        self.config = config
        self.supabase: Client = create_client(config.supabase_url, config.supabase_service_key)

        self.logger = logging.getLogger(config.worker_name)
        self.alert_router = AlertRouter(self.supabase, config, self.logger)

        self.stop_event = asyncio.Event()
        self.websocket = None

        self.session_id: Optional[int] = None
        self.current_mode = "websocket"
        self.connected_status = False
        self.last_message_at: Optional[float] = None
        self.reconnect_count = 0
        self.messages_processed = 0
        self.messages_failed = 0

        self.active_symbols: Set[str] = set()
        self.subscribed_trade_symbols: Set[str] = set()
        self.subscribed_quote_symbols: Set[str] = set()

        self.snapshot_buffer: Dict[str, Dict[str, Any]] = {}
        self.trade_buffer: List[Dict[str, Any]] = []

        self.last_seen_prices: Dict[str, float] = {}
        self._tasks: List[asyncio.Task] = []

    async def run(self) -> None:
        await self.start_session()

        self._tasks = [
            asyncio.create_task(self.connection_manager_loop(), name="connection_manager"),
            asyncio.create_task(self.heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self.snapshot_flush_loop(), name="snapshot_flush"),
            asyncio.create_task(self.subscription_refresh_loop(), name="subscription_refresh"),
            asyncio.create_task(self.feed_status_loop(), name="feed_status"),
            asyncio.create_task(self.staleness_monitor_loop(), name="staleness_monitor"),
        ]

        try:
            await self.stop_event.wait()
        finally:
            for task in self._tasks:
                task.cancel()
            for task in self._tasks:
                with suppress(asyncio.CancelledError):
                    await task
            await self.flush_snapshots()
            await self.flush_trades()
            await self.end_session("stopped")

    async def start_session(self) -> None:
        payload = {
            "worker_name": self.config.worker_name,
            "worker_version": self.config.worker_version,
            "provider_name": self.config.provider_name,
            "environment": self.config.environment,
            "runtime_mode": self.current_mode,
            "session_status": "starting",
            "started_at": utc_now_iso(),
            "last_heartbeat_at": utc_now_iso(),
            "startup_config_json": {
                "heartbeat_interval_seconds": self.config.heartbeat_interval_seconds,
                "snapshot_flush_interval_seconds": self.config.snapshot_flush_interval_seconds,
                "subscription_refresh_interval_seconds": self.config.subscription_refresh_interval_seconds,
                "stale_threshold_seconds": self.config.stale_threshold_seconds,
                "enable_snapshot_writes": self.config.enable_snapshot_writes,
                "enable_trade_writes": self.config.enable_trade_writes,
                "enable_quote_subscriptions": self.config.enable_quote_subscriptions,
            },
            "symbol_count_at_start": 0,
        }
        result = self.supabase.table("market_worker_sessions").insert(payload).execute()
        self.session_id = result.data[0]["id"]
        self.logger.info("Started session %s", self.session_id)

    async def end_session(self, status: str) -> None:
        if not self.session_id:
            return
        self.supabase.table("market_worker_sessions").update({
            "session_status": status,
            "ended_at": utc_now_iso(),
            "last_heartbeat_at": utc_now_iso(),
        }).eq("id", self.session_id).execute()

    async def connection_manager_loop(self) -> None:
        backoff = 1
        while not self.stop_event.is_set():
            try:
                await self.connect_and_consume()
                backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected_status = False
                self.reconnect_count += 1
                self.messages_failed += 1
                self.logger.exception("WebSocket loop error: %s", exc)

                self.write_recovery_event(
                    event_type="reconnect_failed",
                    severity="high",
                    message=f"Connection loop failed: {exc}",
                )
                self.alert_router.send_system_alert(
                    title="Market Feed Connection Error",
                    message=f"{self.config.worker_name} lost connection and will retry.",
                    metadata={"worker": self.config.worker_name, "error": str(exc)},
                )

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def connect_and_consume(self) -> None:
        self.logger.info("Connecting to %s", self.config.ws_url)

        async with websockets.connect(
            self.config.ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=2**22,
        ) as ws:
            self.websocket = ws
            self.connected_status = True
            self.last_message_at = time.time()

            self.write_recovery_event(
                event_type="reconnect_success",
                severity="low",
                message="Connected successfully",
            )

            await self.authenticate()
            await self.refresh_active_symbols()
            await self.sync_subscriptions(force=True)

            self.supabase.table("market_worker_sessions").update({
                "session_status": "active",
                "last_heartbeat_at": utc_now_iso(),
                "symbol_count_at_start": len(self.active_symbols),
            }).eq("id", self.session_id).execute()

            while not self.stop_event.is_set():
                try:
                    raw_message = await asyncio.wait_for(
                        ws.recv(),
                        timeout=self.config.stale_threshold_seconds,
                    )
                    self.last_message_at = time.time()
                    await self.handle_message(raw_message)
                except asyncio.TimeoutError:
                    self.connected_status = False
                    self.write_recovery_event(
                        event_type="websocket_disconnect",
                        severity="medium",
                        message="No messages within stale threshold",
                    )
                    raise ConnectionError("Stale websocket connection")

    async def authenticate(self) -> None:
        assert self.websocket is not None
        await self.websocket.send(json.dumps({
            "action": "auth",
            "params": self.config.polygon_api_key,
        }))
        response = await self.websocket.recv()
        self.logger.info("Auth response: %s", response)

    async def handle_message(self, raw_message: str) -> None:
        try:
            data = json.loads(raw_message)
            if not isinstance(data, list):
                return

            for event in data:
                ev = event.get("ev")
                if ev == "status":
                    continue
                if ev == "T":
                    self.handle_trade_event(event)
                elif ev == "Q":
                    self.handle_quote_event(event)

            self.messages_processed += len(data)
        except Exception as exc:
            self.messages_failed += 1
            self.logger.exception("Message handling failed: %s", exc)
            self.write_provider_error_event(None, f"Message handling failed: {exc}")

    def handle_trade_event(self, event: Dict[str, Any]) -> None:
        symbol = event.get("sym")
        price = event.get("p")
        size = event.get("s")
        trade_time_ms = event.get("t")
        exchange = event.get("x")
        conditions = event.get("c")

        if not symbol or price is None:
            return

        event_time = self.ms_to_iso(trade_time_ms)
        received_at = utc_now_iso()
        latency_ms = self.compute_latency_ms(trade_time_ms)

        previous_snapshot = self.snapshot_buffer.get(symbol, {})
        previous_close = previous_snapshot.get("previous_close")
        prev_price = self.last_seen_prices.get(symbol)

        absolute_change = None
        percent_change = None
        if previous_close not in (None, 0):
            absolute_change = float(price) - float(previous_close)
            percent_change = (absolute_change / float(previous_close)) * 100.0

        snapshot = previous_snapshot.copy()
        snapshot.update({
            "symbol": symbol,
            "worker_session_id": self.session_id,
            "provider_name": self.config.provider_name,
            "environment": self.config.environment,
            "last_price": price,
            "last_trade_size": size,
            "event_time": event_time,
            "received_at": received_at,
            "processed_at": received_at,
            "latency_ms": latency_ms,
            "freshness_status": "live",
            "updated_at": received_at,
            "absolute_change": absolute_change,
            "percent_change": percent_change,
        })
        self.snapshot_buffer[symbol] = snapshot

        if self.config.enable_trade_writes:
            self.trade_buffer.append({
                "symbol": symbol,
                "price": price,
                "size": size,
                "trade_time": event_time,
                "exchange": exchange,
                "conditions_json": conditions,
                "provider_name": self.config.provider_name,
                "created_at": received_at,
            })

        self.maybe_send_price_movement_alert(symbol, price, prev_price)
        self.last_seen_prices[symbol] = float(price)

    def handle_quote_event(self, event: Dict[str, Any]) -> None:
        symbol = event.get("sym")
        bid_price = event.get("bp")
        ask_price = event.get("ap")
        quote_time_ms = event.get("t")

        if not symbol:
            return

        snapshot = self.snapshot_buffer.get(symbol, {})
        received_at = utc_now_iso()
        snapshot.update({
            "symbol": symbol,
            "worker_session_id": self.session_id,
            "provider_name": self.config.provider_name,
            "environment": self.config.environment,
            "bid_price": bid_price,
            "ask_price": ask_price,
            "spread": (ask_price - bid_price) if bid_price is not None and ask_price is not None else None,
            "event_time": self.ms_to_iso(quote_time_ms),
            "received_at": received_at,
            "processed_at": received_at,
            "latency_ms": self.compute_latency_ms(quote_time_ms),
            "freshness_status": "live",
            "updated_at": received_at,
        })
        self.snapshot_buffer[symbol] = snapshot

    def maybe_send_price_movement_alert(
        self,
        symbol: str,
        price: float,
        prev_price: Optional[float],
    ) -> None:
        if prev_price is None:
            return

        diff = abs(float(price) - float(prev_price))
        if diff >= 1.0:
            self.alert_router.send_symbol_alert(
                symbol=symbol,
                alert_type="system_error",
                title=f"{symbol} Price Movement",
                message=f"{symbol} moved from ${prev_price:.2f} to ${float(price):.2f}",
                metadata={
                    "symbol": symbol,
                    "prev_price": prev_price,
                    "price": float(price),
                    "env": self.config.environment,
                    "worker": self.config.worker_name,
                },
            )

    async def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                health_state = self.derive_health_state()
                self.supabase.table("market_worker_health_snapshots").insert({
                    "worker_session_id": self.session_id,
                    "heartbeat_at": utc_now_iso(),
                    "connected_status": self.connected_status,
                    "runtime_mode": self.current_mode,
                    "uptime_seconds": int(time.time()),
                    "subscription_count": len(self.subscribed_trade_symbols),
                    "active_symbols_receiving_data": len(self.snapshot_buffer),
                    "messages_processed": self.messages_processed,
                    "messages_failed": self.messages_failed,
                    "avg_latency_ms": self.estimate_avg_latency_ms(),
                    "current_health_state": health_state,
                    "notes": None,
                }).execute()

                self.supabase.table("market_worker_sessions").update({
                    "session_status": "active" if health_state == "healthy" else health_state,
                    "last_heartbeat_at": utc_now_iso(),
                }).eq("id", self.session_id).execute()
            except Exception as exc:
                self.logger.warning("Heartbeat failed: %s", exc)

            await asyncio.sleep(self.config.heartbeat_interval_seconds)

    async def snapshot_flush_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.flush_snapshots()
                await self.flush_trades()
            except Exception as exc:
                self.logger.warning("Snapshot flush loop failed: %s", exc)
            await asyncio.sleep(self.config.snapshot_flush_interval_seconds)

    async def flush_snapshots(self) -> None:
        if not self.config.enable_snapshot_writes or not self.snapshot_buffer:
            return

        rows = list(self.snapshot_buffer.values())[: self.config.write_batch_max_size]
        started = time.time()

        try:
            self.supabase.table("market_snapshots").upsert(rows).execute()
            duration_ms = int((time.time() - started) * 1000)

            self.supabase.table("market_snapshot_write_logs").insert({
                "worker_session_id": self.session_id,
                "batch_size": len(rows),
                "symbols_written": [r["symbol"] for r in rows],
                "write_started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
                "write_completed_at": utc_now_iso(),
                "write_duration_ms": duration_ms,
                "status": "success",
                "error_message": None,
            }).execute()

            for row in rows:
                self.snapshot_buffer.pop(row["symbol"], None)

        except Exception as exc:
            self.supabase.table("market_snapshot_write_logs").insert({
                "worker_session_id": self.session_id,
                "batch_size": len(rows),
                "symbols_written": [r["symbol"] for r in rows],
                "write_started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
                "write_completed_at": utc_now_iso(),
                "write_duration_ms": int((time.time() - started) * 1000),
                "status": "failed",
                "error_message": str(exc),
            }).execute()

            self.write_recovery_event(
                event_type="snapshot_write_failure",
                severity="high",
                message=f"Snapshot write failed: {exc}",
            )

    async def flush_trades(self) -> None:
        if not self.config.enable_trade_writes or not self.trade_buffer:
            return

        rows = self.trade_buffer[: self.config.write_batch_max_size]
        try:
            self.supabase.table("market_trades").insert(rows).execute()
            del self.trade_buffer[: len(rows)]
        except Exception as exc:
            self.logger.warning("Trade flush failed: %s", exc)

    async def subscription_refresh_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.refresh_active_symbols()
                await self.sync_subscriptions(force=False)
            except Exception as exc:
                self.logger.warning("Subscription refresh failed: %s", exc)
            await asyncio.sleep(self.config.subscription_refresh_interval_seconds)

    async def refresh_active_symbols(self) -> None:
        result = self.supabase.table("market_active_symbol_sets").select(
            "symbol"
        ).eq("environment", self.config.environment).execute()

        self.active_symbols = {
            row["symbol"] for row in (result.data or []) if row.get("symbol")
        }

    async def sync_subscriptions(self, force: bool = False) -> None:
        if self.websocket is None or not self.connected_status:
            return

        to_add_trades = self.active_symbols - self.subscribed_trade_symbols
        to_remove_trades = self.subscribed_trade_symbols - self.active_symbols

        if force or to_add_trades or to_remove_trades:
            if to_add_trades:
                channels = ",".join(f"T.{s}" for s in sorted(to_add_trades))
                await self.websocket.send(json.dumps({"action": "subscribe", "params": channels}))
            if to_remove_trades:
                channels = ",".join(f"T.{s}" for s in sorted(to_remove_trades))
                await self.websocket.send(json.dumps({"action": "unsubscribe", "params": channels}))
            self.subscribed_trade_symbols = set(self.active_symbols)

        if self.config.enable_quote_subscriptions:
            to_add_quotes = self.active_symbols - self.subscribed_quote_symbols
            to_remove_quotes = self.subscribed_quote_symbols - self.active_symbols

            if force or to_add_quotes or to_remove_quotes:
                if to_add_quotes:
                    channels = ",".join(f"Q.{s}" for s in sorted(to_add_quotes))
                    await self.websocket.send(json.dumps({"action": "subscribe", "params": channels}))
                if to_remove_quotes:
                    channels = ",".join(f"Q.{s}" for s in sorted(to_remove_quotes))
                    await self.websocket.send(json.dumps({"action": "unsubscribe", "params": channels}))
                self.subscribed_quote_symbols = set(self.active_symbols)

        self.supabase.table("market_subscription_sync_logs").insert({
            "worker_session_id": self.session_id,
            "requested_at": utc_now_iso(),
            "symbols_requested": sorted(self.active_symbols),
            "symbols_added": sorted(list(to_add_trades)),
            "symbols_removed": sorted(list(to_remove_trades)),
            "final_symbol_count": len(self.subscribed_trade_symbols),
            "status": "success",
            "notes": "force" if force else None,
        }).execute()

    async def feed_status_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.supabase.table("market_feed_status").upsert({
                    "provider_name": self.config.provider_name,
                    "worker_name": self.config.worker_name,
                    "connection_status": "connected" if self.connected_status else "disconnected",
                    "subscription_count": len(self.subscribed_trade_symbols),
                    "symbols_receiving_data": len(self.snapshot_buffer),
                    "last_message_at": self.ts_to_iso(self.last_message_at),
                    "last_snapshot_write_at": utc_now_iso(),
                    "reconnect_count": self.reconnect_count,
                    "messages_processed": self.messages_processed,
                    "messages_failed": self.messages_failed,
                    "current_mode": self.current_mode,
                    "environment": self.config.environment,
                    "updated_at": utc_now_iso(),
                }).execute()
            except Exception as exc:
                self.logger.warning("Feed status write failed: %s", exc)

            await asyncio.sleep(5)

    async def staleness_monitor_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if self.last_message_at:
                    gap = time.time() - self.last_message_at
                    if gap > self.config.stale_threshold_seconds:
                        self.write_recovery_event(
                            event_type="stale_feed",
                            severity="high",
                            message=f"No feed message for {int(gap)} seconds",
                        )
                        self.alert_router.send_system_alert(
                            title="Market Feed Stale",
                            message=f"{self.config.worker_name} has not received market data recently.",
                            metadata={"gap_seconds": int(gap), "worker": self.config.worker_name},
                        )
            except Exception as exc:
                self.logger.warning("Staleness monitor failed: %s", exc)

            await asyncio.sleep(max(5, self.config.stale_threshold_seconds // 2))

    def derive_health_state(self) -> str:
        if not self.connected_status:
            return "disconnected"
        if self.last_message_at is None:
            return "degraded"
        if time.time() - self.last_message_at > self.config.stale_threshold_seconds:
            return "stale"
        if self.messages_failed > max(5, self.messages_processed // 10):
            return "degraded"
        return "healthy"

    def estimate_avg_latency_ms(self) -> Optional[int]:
        latencies = [
            s.get("latency_ms")
            for s in self.snapshot_buffer.values()
            if s.get("latency_ms") is not None
        ]
        if not latencies:
            return None
        return int(sum(latencies) / len(latencies))

    def write_recovery_event(self, event_type: str, severity: str, message: str) -> None:
        try:
            self.supabase.table("market_recovery_events").insert({
                "worker_session_id": self.session_id,
                "event_type": event_type,
                "severity": severity,
                "symbol": None,
                "provider_name": self.config.provider_name,
                "started_at": utc_now_iso(),
                "resolved_at": None,
                "message": message,
                "metadata_json": {
                    "worker_name": self.config.worker_name,
                    "environment": self.config.environment,
                },
            }).execute()
        except Exception as exc:
            self.logger.warning("Recovery event write failed: %s", exc)

    def write_provider_error_event(self, symbol: Optional[str], message: str) -> None:
        try:
            self.supabase.table("market_provider_events").insert({
                "provider_name": self.config.provider_name,
                "event_type": "provider_error",
                "symbol": symbol,
                "payload_json": {"message": message},
                "received_at": utc_now_iso(),
                "processed_at": utc_now_iso(),
                "status": "failed",
                "error_message": message,
            }).execute()
        except Exception as exc:
            self.logger.warning("Provider error event write failed: %s", exc)

    @staticmethod
    def ms_to_iso(value: Optional[int]) -> Optional[str]:
        if not value:
            return None
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()

    @staticmethod
    def compute_latency_ms(event_timestamp_ms: Optional[int]) -> Optional[int]:
        if not event_timestamp_ms:
            return None
        now_ms = int(time.time() * 1000)
        return max(0, now_ms - int(event_timestamp_ms))

    @staticmethod
    def ts_to_iso(value: Optional[float]) -> Optional[str]:
        if value is None:
            return None
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()

    def request_shutdown(self) -> None:
        self.stop_event.set()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
        stream=sys.stdout,
    )


async def main() -> None:
    configure_logging()
    config = Config.from_env()
    worker = MarketWorker(config)

    loop = asyncio.get_running_loop()

    def shutdown_handler() -> None:
        worker.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, shutdown_handler)

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
