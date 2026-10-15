"""Pump.fun and Solana DEX activity monitor.

This is an alert-only tracker. It never signs or submits transactions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

import requests
import websockets

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
DEFAULT_DEX_PROGRAMS = (
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # PumpSwap
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # Raydium CPMM
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM v4
)

HELIUS_HTTP = "https://mainnet.helius-rpc.com/"
HELIUS_WS = "wss://mainnet.helius-rpc.com/"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"
JITO_TIP_ACCOUNTS_URL = "https://mainnet.block-engine.jito.wtf/api/v1/getTipAccounts"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("solana-tracker")


def env_decimal(name: str, default: str) -> Decimal:
    try:
        value = Decimal(os.getenv(name, default))
        if not value.is_finite() or value < 0:
            raise ValueError
        return value
    except (InvalidOperation, ValueError):
        raise SystemExit(f"{name} must be a non-negative number.")


def env_int(name: str, default: str) -> int:
    try:
        value = int(os.getenv(name, default))
        if value < 1:
            raise ValueError
        return value
    except ValueError:
        raise SystemExit(f"{name} must be a positive integer.")


@dataclass(frozen=True)
class Config:
    helius_api_key: str
    telegram_token: str
    telegram_chat_id: str | None
    min_liquidity_usd: Decimal
    max_dev_holding_pct: Decimal
    max_top10_pct: Decimal
    min_swap_usd: Decimal
    candidate_minutes: int
    recheck_seconds: int
    dex_programs: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "Config":
        required = ("HELIUS_API_KEY", "TELEGRAM_BOT_TOKEN")
        missing = [name for name in required if not os.getenv(name, "").strip()]
        if missing:
            names = ", ".join(missing)
            raise SystemExit(
                f"Missing required Replit Secrets: {names}. "
                "Add them in Tools > Secrets, then restart this workflow."
            )

        custom_programs = os.getenv("DEX_PROGRAM_IDS", "").strip()
        dex_programs = tuple(
            item.strip()
            for item in custom_programs.split(",")
            if item.strip()
        ) if custom_programs else DEFAULT_DEX_PROGRAMS

        return cls(
            helius_api_key=os.environ["HELIUS_API_KEY"].strip(),
            telegram_token=os.environ["TELEGRAM_BOT_TOKEN"].strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip() or None,
            min_liquidity_usd=env_decimal("MIN_LIQUIDITY_USD", "10000"),
            max_dev_holding_pct=env_decimal("MAX_DEV_HOLDING_PCT", "10"),
            max_top10_pct=env_decimal("MAX_TOP10_PCT", "90"),
            min_swap_usd=env_decimal("MIN_SWAP_USD", "5000"),
            candidate_minutes=env_int("CANDIDATE_MINUTES", "10"),
            recheck_seconds=env_int("RECHECK_SECONDS", "30"),
            dex_programs=dex_programs,
        )


class HeliusRpc:
    def __init__(self, api_key: str) -> None:
        self.http_url = f"{HELIUS_HTTP}?{urlencode({'api-key': api_key})}"
        self.api_key = api_key
        self._request_id = 0

    def call(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
            "params": params,
        }
        try:
            response = requests.post(self.http_url, json=payload, timeout=20)
            response.raise_for_status()
            body = response.json()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise RuntimeError(f"Helius request failed (HTTP {status}).") from None
        except (requests.RequestException, ValueError) as exc:
            # Never include the request URL in logs: it contains the API key.
            raise RuntimeError(f"Helius request failed ({type(exc).__name__}).") from None
        if "error" in body:
            error = body["error"]
            raise RuntimeError(f"Helius RPC {method} error: {error.get('message', 'unknown error')}")
        return body.get("result")

    def get_transaction(self, signature: str) -> dict[str, Any] | None:
        for attempt in range(4):
            result = self.call(
                "getTransaction",
                [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "confirmed",
                        "maxSupportedTransactionVersion": 1,
                    },
                ],
            )
            if result:
                return result
            if attempt < 3:
                time.sleep(0.4 * (attempt + 1))
        return None

    def holder_metrics(self, mint: str, creator: str) -> tuple[Decimal, Decimal]:
        supply_result = self.call("getTokenSupply", [mint, {"commitment": "confirmed"}])
        supply = int(supply_result["value"]["amount"])
        if supply <= 0:
            raise RuntimeError("Token supply is unavailable.")

        largest_result = self.call(
            "getTokenLargestAccounts",
            [mint, {"commitment": "confirmed"}],
        )
        largest = largest_result.get("value", [])
        top10_raw = sum(int(item["amount"]) for item in largest[:10])
        top10_pct = Decimal(top10_raw) * 100 / Decimal(supply)

        creator_accounts = self.call(
            "getTokenAccountsByOwner",
            [
                creator,
                {"mint": mint},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
        )
        creator_raw = 0
        for account in creator_accounts.get("value", []):
            try:
                creator_raw += int(
                    account["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
                )
            except (KeyError, TypeError, ValueError):
                continue
        dev_pct = Decimal(creator_raw) * 100 / Decimal(supply)
        return dev_pct, top10_pct


class DexScreener:
    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, dict[str, Any] | None]] = {}

    def best_pair(self, mint: str) -> dict[str, Any] | None:
        now = time.monotonic()
        cached = self._cache.get(mint)
        if cached and now - cached[0] < 20:
            return cached[1]
        try:
            response = requests.get(
                DEXSCREENER_TOKEN_URL.format(mint),
                timeout=12,
                headers={"User-Agent": "solana-pump-dex-tracker/1.0"},
            )
            response.raise_for_status()
            pairs = response.json().get("pairs") or []
        except (requests.RequestException, ValueError, TypeError):
            log.warning("DexScreener lookup failed for mint %s.", short_key(mint))
            self._cache[mint] = (now, None)
            return None

        solana_pairs = [pair for pair in pairs if pair.get("chainId") == "solana"]
        solana_pairs.sort(
            key=lambda pair: float((pair.get("liquidity") or {}).get("usd") or 0),
            reverse=True,
        )
        result = solana_pairs[0] if solana_pairs else None
        self._cache[mint] = (now, result)
        return result


class Telegram:
    def __init__(self, token: str, chat_id: str | None) -> None:
        self.token = token
        self.chat_id = chat_id
        self._offset = 0
        self._url = f"https://api.telegram.org/bot{token}"

    async def send(self, text: str) -> bool:
        if not self.chat_id:
            log.info("Telegram chat not set. Open the bot and send /start to enable alerts.")
            return False
        try:
            response = await asyncio.to_thread(
                requests.post,
                f"{self._url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            if not response.ok:
                log.warning("Telegram send failed (HTTP %s).", response.status_code)
                return False
            return True
        except requests.RequestException as exc:
            log.warning("Telegram send failed (%s).", type(exc).__name__)
            return False

    async def discover_chat(self) -> None:
        """Use /start updates to find the private chat without exposing a chat ID."""
        while not self.chat_id:
            try:
                response = await asyncio.to_thread(
                    requests.get,
                    f"{self._url}/getUpdates",
                    params={"timeout": 20, "offset": self._offset},
                    timeout=30,
                )
                if not response.ok:
                    log.warning("Telegram update polling failed (HTTP %s).", response.status_code)
                    await asyncio.sleep(5)
                    continue
                body = response.json()
                for update in body.get("result", []):
                    self._offset = max(self._offset, int(update["update_id"]) + 1)
                    message = update.get("message") or update.get("edited_message") or {}
                    text = str(message.get("text", "")).strip()
                    chat = message.get("chat") or {}
                    if text.split(" ", 1)[0].split("@", 1)[0] == "/start" and chat.get("id"):
                        self.chat_id = str(chat["id"])
                        log.info("Telegram chat connected; sending a startup message.")
                        await self.send(
                            "Solana Pump.fun / DEX izleyicisi etkin. "
                            "Bu bot yalnızca izleme ve uyarı yapar; işlem göndermez."
                        )
                        break
            except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
                log.warning("Telegram update polling failed (%s).", type(exc).__name__)
                await asyncio.sleep(5)


@dataclass
class TokenCandidate:
    mint: str
    creator: str
    signature: str
    created_at: float = field(default_factory=time.monotonic)
    create_jito_tip_sol: Decimal = Decimal("0")
    notified_clean: bool = False
    last_volume_alert: float = 0.0
    eval_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def short_key(value: str, size: int = 5) -> str:
    if len(value) <= size * 2:
        return value
    return f"{value[:size]}…{value[-size:]}"


def transaction_signers(tx: dict[str, Any]) -> list[str]:
    message = tx.get("transaction", {}).get("message", {})
    keys = message.get("accountKeys", [])
    signers: list[str] = []
    for key in keys:
        if isinstance(key, dict):
            if key.get("signer"):
                signers.append(str(key.get("pubkey", "")))
        elif isinstance(key, str):
            # jsonParsed normally returns objects; leave strings for legacy responses.
            if not signers:
                signers.append(key)
    return [key for key in signers if key]


def transaction_mints(tx: dict[str, Any]) -> list[str]:
    meta = tx.get("meta") or {}
    pre_mints = {
        item.get("mint")
        for item in meta.get("preTokenBalances", [])
        if item.get("mint")
    }
    return list(
        dict.fromkeys(
            item.get("mint")
            for item in meta.get("postTokenBalances", [])
            if item.get("mint")
            and item.get("mint") not in pre_mints
            and item.get("mint") != "So11111111111111111111111111111111111111112"
        )
    )


def token_volume_ui(tx: dict[str, Any], mint: str) -> Decimal:
    """Estimate swapped token size from owner balance changes (not an exact fill feed)."""
    meta = tx.get("meta") or {}
    pre: dict[str, int] = {}
    post: dict[str, int] = {}
    decimals = 0

    for field_name, target in (
        ("preTokenBalances", pre),
        ("postTokenBalances", post),
    ):
        for entry in meta.get(field_name, []):
            if entry.get("mint") != mint:
                continue
            amount = entry.get("uiTokenAmount", {})
            decimals = int(amount.get("decimals", decimals))
            owner = entry.get("owner") or f"account:{entry.get('accountIndex', '?')}"
            target[owner] = target.get(owner, 0) + int(amount.get("amount", "0"))

    owners = set(pre) | set(post)
    moved_raw = sum(abs(post.get(owner, 0) - pre.get(owner, 0)) for owner in owners) // 2
    if decimals < 0 or decimals > 18:
        return Decimal("0")
    return Decimal(moved_raw) / (Decimal(10) ** decimals)


class Tracker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.rpc = HeliusRpc(config.helius_api_key)
        self.dex = DexScreener()
        self.telegram = Telegram(config.telegram_token, config.telegram_chat_id)
        self.candidates: dict[str, TokenCandidate] = {}
        self.jito_tip_accounts: set[str] = set()
        self.seen_signatures: set[str] = set()
        self.signature_order: deque[str] = deque()

    def remember_signature(self, signature: str) -> bool:
        if signature in self.seen_signatures:
            return False
        self.seen_signatures.add(signature)
        self.signature_order.append(signature)
        if len(self.signature_order) > 10_000:
            self.seen_signatures.discard(self.signature_order.popleft())
        return True

    def load_jito_tip_accounts(self) -> None:
        try:
            response = requests.post(
                JITO_TIP_ACCOUNTS_URL,
                json={"jsonrpc": "2.0", "id": 1, "method": "getTipAccounts", "params": []},
                timeout=10,
            )
            response.raise_for_status()
            self.jito_tip_accounts = set(response.json().get("result", []))
            log.info("Loaded %d public Jito tip accounts.", len(self.jito_tip_accounts))
        except (requests.RequestException, ValueError, TypeError):
            log.warning("Could not load Jito tip accounts; Jito tip checks are unavailable.")

    def jito_tip_sol(self, tx: dict[str, Any]) -> Decimal:
        if not self.jito_tip_accounts:
            return Decimal("0")
        message = tx.get("transaction", {}).get("message", {})
        keys = [
            str(item.get("pubkey", "")) if isinstance(item, dict) else str(item)
            for item in message.get("accountKeys", [])
        ]
        meta = tx.get("meta") or {}
        pre = meta.get("preBalances", [])
        post = meta.get("postBalances", [])
        lamports = 0
        for index, key in enumerate(keys):
            if key in self.jito_tip_accounts and index < len(pre) and index < len(post):
                lamports += max(0, int(post[index]) - int(pre[index]))
        return Decimal(lamports) / Decimal(1_000_000_000)

    async def get_transaction(self, signature: str) -> dict[str, Any] | None:
        try:
            return await asyncio.to_thread(self.rpc.get_transaction, signature)
        except (RuntimeError, KeyError, TypeError, ValueError) as exc:
            detail = str(exc).replace(self.config.helius_api_key, "[redacted]")
            log.warning("Transaction lookup failed: %s.", detail)
            return None

    def parse_new_candidate(
        self,
        tx: dict[str, Any],
        signature: str,
    ) -> TokenCandidate | None:
        mints = transaction_mints(tx)
        signers = transaction_signers(tx)
        if not mints or not signers:
            return None
        return TokenCandidate(
            mint=mints[0],
            creator=signers[0],
            signature=signature,
            create_jito_tip_sol=self.jito_tip_sol(tx),
        )

    async def watch_candidate(self, candidate: TokenCandidate) -> None:
        deadline = time.monotonic() + self.config.candidate_minutes * 60
        while time.monotonic() < deadline and not candidate.notified_clean:
            try:
                result = await self.evaluate(candidate)
                if result and result["clean"]:
                    await self.notify_clean(candidate, result)
                    if candidate.notified_clean:
                        return
            except (RuntimeError, KeyError, TypeError, ValueError) as exc:
                log.warning(
                    "Risk check failed for %s (%s).",
                    short_key(candidate.mint),
                    type(exc).__name__,
                )
            await asyncio.sleep(self.config.recheck_seconds)

    async def evaluate(self, candidate: TokenCandidate) -> dict[str, Any] | None:
        async with candidate.eval_lock:
            pair = await asyncio.to_thread(self.dex.best_pair, candidate.mint)
            if not pair:
                return None

            try:
                liquidity = Decimal(str((pair.get("liquidity") or {}).get("usd") or "0"))
                price = Decimal(str(pair.get("priceUsd") or "0"))
                dev_pct, top10_pct = await asyncio.to_thread(
                    self.rpc.holder_metrics,
                    candidate.mint,
                    candidate.creator,
                )
            except (RuntimeError, KeyError, TypeError, ValueError, InvalidOperation) as exc:
                raise RuntimeError(f"Risk data unavailable ({type(exc).__name__}).") from None

            reasons = []
            if liquidity < self.config.min_liquidity_usd:
                reasons.append("liquidity below threshold")
            if dev_pct > self.config.max_dev_holding_pct:
                reasons.append("developer wallet concentration above threshold")
            if top10_pct > self.config.max_top10_pct:
                reasons.append("top-10 token-account concentration above threshold")
            return {
                "clean": not reasons,
                "reasons": reasons,
                "pair": pair,
                "liquidity": liquidity,
                "price": price,
                "dev_pct": dev_pct,
                "top10_pct": top10_pct,
            }

    def jito_summary(self, candidate: TokenCandidate, observed_tip: Decimal = Decimal("0")) -> str:
        tip = max(candidate.create_jito_tip_sol, observed_tip)
        if tip > 0:
            return (
                f"Jito tip: {tip:.4f} SOL gözlendi "
                "(bundle üyeliği zincir verisinden kesin doğrulanamaz)"
            )
        if self.jito_tip_accounts:
            return "İlgili işlemde bilinen Jito tip hesabına ödeme gözlenmedi"
        return "Jito kontrolü şu anda kullanılamıyor"

    async def notify_clean(
        self,
        candidate: TokenCandidate,
        result: dict[str, Any],
    ) -> None:
        if candidate.notified_clean:
            return
        pair = result["pair"]
        message = (
            "✅ Filtreleri geçen Solana tokeni\n"
            f"Ad: {pair.get('baseToken', {}).get('name', 'Bilinmiyor')}\n"
            f"Sembol: {pair.get('baseToken', {}).get('symbol', '—')}\n"
            f"Mint: {candidate.mint}\n"
            f"Geliştirici cüzdanı: {result['dev_pct']:.2f}%\n"
            f"En büyük 10 token hesabı: {result['top10_pct']:.2f}%\n"
            f"Likidite: ${result['liquidity']:,.0f}\n"
            f"Fiyat: ${result['price']:.10f}\n"
            f"{self.jito_summary(candidate)}\n"
            f"İşlem: https://solscan.io/tx/{candidate.signature}\n"
            f"DEX: {pair.get('url', 'DexScreener')}\n"
            "Not: Filtreler zincir üstü göstergelerdir; güvenlik veya kâr garantisi değildir."
        )
        if await self.telegram.send(message):
            candidate.notified_clean = True
            log.info("Clean-filter alert emitted for %s.", short_key(candidate.mint))

    async def process_create(self, signature: str) -> None:
        tx = await self.get_transaction(signature)
        if not tx or (tx.get("meta") or {}).get("err") is not None:
            return
        candidate = self.parse_new_candidate(tx, signature)
        if not candidate:
            log.info("Create event had no identifiable mint/creator (%s).", short_key(signature))
            return
        if candidate.mint in self.candidates:
            return
        self.candidates[candidate.mint] = candidate
        log.info(
            "New Pump.fun token candidate: %s (creator %s).",
            short_key(candidate.mint),
            short_key(candidate.creator),
        )
        asyncio.create_task(self.watch_candidate(candidate))

    async def process_swap(self, signature: str) -> None:
        tx = await self.get_transaction(signature)
        if not tx or (tx.get("meta") or {}).get("err") is not None:
            return
        tip = self.jito_tip_sol(tx)
        touched: set[str] = set()
        for mint in {
            item.get("mint")
            for field_name in ("preTokenBalances", "postTokenBalances")
            for item in (tx.get("meta") or {}).get(field_name, [])
            if item.get("mint")
        }:
            candidate = self.candidates.get(mint)
            if not candidate or mint in touched:
                continue
            touched.add(mint)
            try:
                volume_tokens = token_volume_ui(tx, mint)
                pair = await asyncio.to_thread(self.dex.best_pair, mint)
                if not pair:
                    continue
                price = Decimal(str(pair.get("priceUsd") or "0"))
                volume_usd = volume_tokens * price
                if volume_usd < self.config.min_swap_usd:
                    continue
                result = await self.evaluate(candidate)
                if not result or not result["clean"]:
                    continue
                now = time.monotonic()
                if now - candidate.last_volume_alert < 180:
                    continue
                message = (
                    "📈 Büyük DEX işlem hareketi\n"
                    f"Token: {pair.get('baseToken', {}).get('symbol', short_key(mint))}\n"
                    f"Mint: {mint}\n"
                    f"Tahmini işlem büyüklüğü: ${volume_usd:,.0f}\n"
                    f"Likidite: ${result['liquidity']:,.0f}\n"
                    f"Geliştirici cüzdanı: {result['dev_pct']:.2f}%\n"
                    f"{self.jito_summary(candidate, tip)}\n"
                    f"İşlem: https://solscan.io/tx/{signature}\n"
                    "Hacim tahmini token bakiye değişiminden hesaplanır."
                )
                if await self.telegram.send(message):
                    candidate.last_volume_alert = now
                    log.info(
                        "Large-swap alert emitted for %s (~$%s).",
                        short_key(mint),
                        f"{volume_usd:,.0f}",
                    )
            except (RuntimeError, KeyError, TypeError, ValueError, InvalidOperation) as exc:
                log.warning("Swap analysis failed for %s (%s).", short_key(mint), type(exc).__name__)

    async def handle_log(self, program_id: str, result: dict[str, Any]) -> None:
        value = result.get("value", {})
        signature = value.get("signature")
        logs = value.get("logs") or []
        if not signature or value.get("err") is not None or not self.remember_signature(signature):
            return
        joined_logs = "\n".join(logs).lower()

        if program_id == PUMP_FUN_PROGRAM and (
            "instruction: create" in joined_logs
            or "instruction: create_v2" in joined_logs
        ):
            await self.process_create(signature)
            return

        if any(term in joined_logs for term in ("instruction: buy", "instruction: sell", "instruction: swap")):
            await self.process_swap(signature)

    async def stream(self) -> None:
        programs = tuple(dict.fromkeys((PUMP_FUN_PROGRAM, *self.config.dex_programs)))
        query = urlencode({"api-key": self.config.helius_api_key})
        ws_url = f"{HELIUS_WS}?{query}"
        delay = 1

        while True:
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=60,
                    # Keep sending Helius' recommended one-minute heartbeat, but don't
                    # force-close on a missing pong through a proxy/RPC gateway.
                    ping_timeout=None,
                    close_timeout=10,
                    max_size=5_000_000,
                ) as websocket:
                    log.info("Helius WebSocket connected; subscribing to %d programs.", len(programs))
                    self._subscription_programs = {}
                    for index, program_id in enumerate(programs, start=1):
                        await websocket.send(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "id": index,
                                    "method": "logsSubscribe",
                                    "params": [
                                        {"mentions": [program_id]},
                                        {"commitment": "confirmed"},
                                    ],
                                }
                            )
                        )
                    acknowledged = 0
                    delay = 1
                    async for raw_message in websocket:
                        try:
                            message = json.loads(raw_message)
                            if (
                                "id" in message
                                and isinstance(message.get("result"), int)
                                and 1 <= int(message["id"]) <= len(programs)
                            ):
                                request_id = int(message["id"])
                                self._subscription_programs[message["result"]] = programs[request_id - 1]
                                acknowledged += 1
                                if acknowledged == len(programs):
                                    log.info("Helius subscriptions confirmed.")
                                continue
                            if message.get("method") != "logsNotification":
                                continue
                            params = message.get("params", {})
                            program_id = self._subscription_programs.get(params.get("subscription"))
                            if program_id:
                                await self.handle_log(program_id, params.get("result", {}))
                        except (json.JSONDecodeError, TypeError, ValueError, KeyError):
                            log.debug("Ignoring malformed WebSocket notification.")
            except asyncio.CancelledError:
                raise
            except websockets.ConnectionClosed as exc:
                log.warning(
                    "Helius WebSocket closed (code=%s, reason=%s); reconnecting in %ss.",
                    exc.code,
                    exc.reason or "no reason supplied",
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            except Exception as exc:
                # The websocket URL includes the Helius key; never print exception text or URL.
                log.warning(
                    "Helius WebSocket disconnected (%s); reconnecting in %ss.",
                    type(exc).__name__,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def run(self) -> None:
        self._subscription_programs = {}
        self.load_jito_tip_accounts()

        if self.telegram.chat_id:
            await self.telegram.send(
                "Solana Pump.fun / DEX izleyicisi başlatıldı. "
                "Bu bot yalnızca izleme ve uyarı yapar; işlem göndermez."
            )
        else:
            asyncio.create_task(self.telegram.discover_chat())
            log.info("Telegram'da bota /start gönderin; bildirim sohbeti otomatik bulunacak.")

        # websockets subscription IDs are server-assigned. Capture each ID from its response,
        # then map it to the program used for that subscribe request.
        await self.stream()


def main() -> None:
    config = Config.from_env()
    tracker = Tracker(config)

    try:
        asyncio.run(tracker.run())
    except KeyboardInterrupt:
        log.info("Tracker stopped.")


if __name__ == "__main__":
    main()
