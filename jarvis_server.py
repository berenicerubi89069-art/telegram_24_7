#!/usr/bin/env python3
"""
J.A.R.V.I.S. — Servidor Central 24/7 en la Nube
================================================
Este módulo se ejecuta en un servidor en la nube (VPS, Render, Railway, etc.).
Mantiene el Bot de Telegram activo las 24 horas del día con Google Gemini
y provee un Gateway WebSocket seguro para recibir la conexión de tu laptop
cuando la enciendes, permitiéndote controlarla remotamente.

Variables de entorno requeridas:
    TELEGRAM_BOT_TOKEN          — Token del bot (@BotFather)
    TELEGRAM_AUTHORIZED_USER_ID — Tu ID numérico de Telegram
    GEMINI_API_KEY              — Clave de API de Google Gemini
    GEMINI_MODEL                — (Opcional) Modelo de Gemini (default: gemini-2.5-flash)
    BRIDGE_SECRET               — Token secreto para autenticar tu laptop
    PORT                        — (Opcional) Puerto del servidor (default: 8888)
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import re
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Final

import aiohttp
from aiohttp import web
from telegram import Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------------------------
# Logging estructurado
# ---------------------------------------------------------------------------
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger: Final = logging.getLogger("jarvis.server")

# ---------------------------------------------------------------------------
# Cargar variables locales de .env si existe
# ---------------------------------------------------------------------------
def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            if k and k not in os.environ:
                os.environ[k] = v

_load_env_file()

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ServerConfig:
    bot_token: str
    authorized_user_id: int
    gemini_api_key: str
    gemini_model: str
    bridge_secret: str
    port: int

    @classmethod
    def from_env(cls) -> ServerConfig:
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        auth_user = os.getenv("TELEGRAM_AUTHORIZED_USER_ID", "0").strip()
        gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
        gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
        bridge_secret = os.getenv("BRIDGE_SECRET", "jarvis_secret_key_2026").strip()
        port = int(os.getenv("PORT", "8888"))

        errores: list[str] = []
        if not bot_token:
            errores.append("TELEGRAM_BOT_TOKEN")
        if not auth_user or auth_user == "0":
            errores.append("TELEGRAM_AUTHORIZED_USER_ID")
        if not gemini_key:
            errores.append("GEMINI_API_KEY")

        if errores:
            logger.critical("Variables requeridas faltantes: %s", ", ".join(errores))
            sys.exit(1)

        return cls(
            bot_token=bot_token,
            authorized_user_id=int(auth_user),
            gemini_api_key=gemini_key,
            gemini_model=gemini_model,
            bridge_secret=bridge_secret,
            port=port,
        )

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
BRIDGE_ACTION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\[BRIDGE_ACTION:\s*(\{.*?\})\]", re.DOTALL
)
MAX_HISTORY_LENGTH: Final[int] = 12
GEMINI_BASE_URL: Final[str] = "https://generativelanguage.googleapis.com/v1beta"
EDGE_TTS_VOICE: Final[str] = os.getenv("JARVIS_TTS_VOICE", "es-ES-AlvaroNeural")

# ---------------------------------------------------------------------------
# Gestor de Conexión con la Laptop (WebSocket Bridge)
# ---------------------------------------------------------------------------
class LaptopBridgeManager:
    """Administra la sesión WebSocket abierta por la laptop local."""

    def __init__(self, expected_secret: str) -> None:
        self._expected_secret = expected_secret
        self._active_ws: web.WebSocketResponse | None = None
        self._lock = asyncio.Lock()
        self._pending_responses: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._telegram_bot: Any = None
        self._authorized_chat_id: int = 0
        self.laptop_info: dict[str, Any] = {}

    def set_telegram_context(self, bot: Any, chat_id: int) -> None:
        self._telegram_bot = bot
        self._authorized_chat_id = chat_id

    @property
    def is_connected(self) -> bool:
        return self._active_ws is not None and not self._active_ws.closed

    async def register_connection(self, ws: web.WebSocketResponse, client_info: dict[str, Any]) -> None:
        async with self._lock:
            # Si existía una conexión previa, cerrarla limpiamente
            if self._active_ws and not self._active_ws.closed:
                try:
                    await self._active_ws.close()
                except Exception:
                    pass
            self._active_ws = ws
            self.laptop_info = client_info

        logger.info("🟢 Estación de trabajo enlazada: %s", client_info.get("hostname", "Laptop"))

        # Notificar a Telegram
        if self._telegram_bot and self._authorized_chat_id:
            try:
                hostname = client_info.get("hostname", "Laptop")
                os_name = client_info.get("os", "Windows")
                text = (
                    "🟢 *J.A.R.V.I.S. — Enlace Cuántico Establecido*\n\n"
                    f"Señor, se ha detectado enlace con su estación de trabajo local:\n"
                    f"• *Equipo:* `{hostname}` ({os_name})\n"
                    f"• *Estado:* `ONLINE (En línea)`\n\n"
                    "El control remoto de su computadora está plenamente operativo."
                )
                await self._telegram_bot.send_message(
                    chat_id=self._authorized_chat_id,
                    text=text,
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception as exc:
                logger.error("Error enviando alerta de conexión a Telegram: %s", exc)

    async def unregister_connection(self) -> None:
        async with self._lock:
            self._active_ws = None
            self.laptop_info = {}

        logger.info("🔴 Estación de trabajo desconectada.")

        # Notificar a Telegram
        if self._telegram_bot and self._authorized_chat_id:
            try:
                text = (
                    "🔴 *J.A.R.V.I.S. — Estación de Trabajo Desconectada*\n\n"
                    "Señor, el enlace con su computadora local ha finalizado (Laptop OFFLINE).\n"
                    "Mis funciones cognitivas en la nube continúan activas."
                )
                await self._telegram_bot.send_message(
                    chat_id=self._authorized_chat_id,
                    text=text,
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception as exc:
                logger.error("Error enviando alerta de desconexión a Telegram: %s", exc)

    async def execute_action(
        self, action: str, params: dict[str, Any] | None = None, timeout: float = 25.0
    ) -> dict[str, Any]:
        """Envía una acción a la laptop remota y espera su resultado."""
        if not self.is_connected:
            return {
                "success": False,
                "online": False,
                "error": "La estación de trabajo local (laptop) se encuentra apagada o fuera de línea.",
            }

        msg_id = f"cmd_{asyncio.get_event_loop().time()}"
        payload = {
            "id": msg_id,
            "action": action,
            "params": params or {},
        }

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending_responses[msg_id] = future

        try:
            assert self._active_ws is not None
            await self._active_ws.send_str(json.dumps(payload))
            result = await asyncio.wait_for(future, timeout=timeout)
            result["online"] = True
            return result
        except asyncio.TimeoutError:
            return {
                "success": False,
                "online": True,
                "error": f"Tiempo de espera agotado ({timeout}s) esperando respuesta de la laptop.",
            }
        except Exception as exc:
            return {
                "success": False,
                "online": False,
                "error": f"Fallo al enviar directiva a la laptop: {exc}",
            }
        finally:
            self._pending_responses.pop(msg_id, None)

    def handle_incoming_message(self, raw_text: str) -> None:
        """Procesa una respuesta recibida de la laptop."""
        try:
            data = json.loads(raw_text)
            msg_id = data.get("id")
            if msg_id and msg_id in self._pending_responses:
                future = self._pending_responses[msg_id]
                if not future.done():
                    future.set_result(data.get("result", {}))
        except Exception as exc:
            logger.error("Error deserializando mensaje de la laptop: %s", exc)


# ---------------------------------------------------------------------------
# Instrucción de sistema para Gemini
# ---------------------------------------------------------------------------
def _build_system_instruction() -> str:
    return (
        "Eres J.A.R.V.I.S., el asistente de inteligencia artificial personal táctico inspirado en JARVIS.\n"
        "Tu personalidad es la de un mayordomo de élite: sumamente elegante, respetuoso, directo, "
        "inteligente, culto, refinado y sin rodeos innecesarios.\n"
        "Hablas y respondes SIEMPRE en español formal impecable.\n"
        "Te diriges al usuario habitualmente como «Señor».\n\n"
        "NORMAS:\n"
        "1. Eres conciso, eficiente y práctico en mensajería móvil.\n"
        "2. El sistema está dividido en dos partes: el Servidor en la Nube (donde resides 24/7) "
        "y la Estación de Trabajo Local (la laptop Windows del Señor).\n"
        "3. Si el usuario te pide expresamente interactuar con su computadora física "
        "(abrir programas, listar archivos, leer archivos, bloquear la PC, tomar captura de pantalla o diagnósticos), "
        "puedes emitir una directiva en tu respuesta con la siguiente sintaxis exacta:\n"
        '[BRIDGE_ACTION: {"action": "open_item", "params": {"target": "notepad.exe"}}]\n'
        '[BRIDGE_ACTION: {"action": "list_files", "params": {"path": ""}}]\n'
        '[BRIDGE_ACTION: {"action": "read_file", "params": {"filename": "nombre_archivo"}}]\n'
        '[BRIDGE_ACTION: {"action": "write_file", "params": {"filename": "ruta", "content": "texto"}}]\n'
        '[BRIDGE_ACTION: {"action": "screenshot", "params": {}}]\n'
        '[BRIDGE_ACTION: {"action": "lock_workstation", "params": {}}]\n'
        '[BRIDGE_ACTION: {"action": "system_diagnostics", "params": {}}]\n'
        '[BRIDGE_ACTION: {"action": "run_command", "params": {"command": "dir"}}]\n'
        "Si la laptop está desconectada, el sistema informará al Señor formalmente."
    )


# ---------------------------------------------------------------------------
# Cliente de Gemini
# ---------------------------------------------------------------------------
class GeminiClient:
    def __init__(self, api_key: str, model: str) -> None:
        self._api_key = api_key
        self._model = model
        self._system_instruction = _build_system_instruction()

    async def generate(self, history: list[dict[str, str]]) -> str:
        models_to_try = [self._model]
        for fallback in ("gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash"):
            if fallback not in models_to_try:
                models_to_try.append(fallback)

        contents = [
            {"role": "model" if t["role"] == "model" else "user", "parts": [{"text": t["text"]}]}
            for t in history
        ]
        payload = {
            "systemInstruction": {"parts": [{"text": self._system_instruction}]},
            "contents": contents,
            "generationConfig": {"temperature": 0.6, "maxOutputTokens": 3000},
        }

        last_error: Exception | None = None
        async with aiohttp.ClientSession() as session:
            for model_name in models_to_try:
                endpoint = (
                    f"{GEMINI_BASE_URL}/models/{model_name}:generateContent?key={self._api_key}"
                )
                try:
                    async with session.post(
                        endpoint, json=payload, timeout=aiohttp.ClientTimeout(total=45)
                    ) as resp:
                        if resp.status != 200:
                            text_err = await resp.text()
                            logger.warning("Modelo %s falló en servidor (HTTP %d): %s", model_name, resp.status, text_err[:120])
                            continue
                        data = await resp.json()

                    result = "".join(
                        p.get("text", "") for p in data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
                    )
                    if result:
                        return result
                except Exception as exc:
                    last_error = exc
                    logger.warning("Excepción con modelo %s en servidor: %s", model_name, exc)
                    continue

        raise RuntimeError(f"Error al contactar Gemini con todos los modelos: {last_error}")


# ---------------------------------------------------------------------------
# Bot de Telegram
# ---------------------------------------------------------------------------
class JarvisServer:
    def __init__(self, config: ServerConfig) -> None:
        self.config = config
        self.bridge = LaptopBridgeManager(config.bridge_secret)
        self.gemini = GeminiClient(config.gemini_api_key, config.gemini_model)
        self.history: dict[int, list[dict[str, str]]] = defaultdict(list)
        self.tg_app: Application | None = None

    def auth_check(self, update: Update) -> bool:
        user_id = update.effective_user.id if update.effective_user else 0
        return user_id == self.config.authorized_user_id

    # -- Comandos Telegram --
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        status = "🟢 ONLINE" if self.bridge.is_connected else "🔴 OFFLINE"
        msg = (
            "🎩 *J.A.R.V.I.S. — Sistema Central en la Nube*\n\n"
            "Saludos, Señor. Me encuentro plenamente operativo en su servidor 24/7.\n\n"
            f"📡 *Estado de su Laptop:* `{status}`\n\n"
            "Comandos disponibles:\n"
            "• `/status` — Diagnóstico del enlace y del servidor.\n"
            "• `/abrir <programa/archivo>` — Abrir app o archivo en su laptop.\n"
            "• `/captura` — Tomar captura de pantalla de su PC.\n"
            "• `/bloquear` — Bloquear la pantalla de su laptop.\n"
            "• `/cmd <comando>` — Ejecutar comando PowerShell en su PC.\n"
            "• `/archivos [ruta]` — Listar archivos del escritorio o directorio.\n"
            "• `/reset` — Reiniciar memoria conversacional de JARVIS.\n\n"
            "También puede conversar conmigo de forma natural sobre cualquier tema."
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        if self.bridge.is_connected:
            info = self.bridge.laptop_info
            diag = await self.bridge.execute_action("system_diagnostics", timeout=5.0)
            detalles = ""
            if diag.get("success"):
                detalles = (
                    f"\n• *Disco Libre:* `{diag.get('disk_free_gb')} GB` / `{diag.get('disk_total_gb')} GB`\n"
                    f"• *Procesador:* `{diag.get('processor', 'N/D')}`"
                )
            text = (
                "📊 *Informe de Estado del Sistema:*\n\n"
                "• *Servidor Central:* 🟢 Activo (24/7)\n"
                f"• *Estación Local (Laptop):* 🟢 ONLINE\n"
                f"• *Equipo:* `{info.get('hostname', 'Windows PC')}` ({info.get('os', 'Windows')})"
                f"{detalles}\n\n"
                "Todos los protocolos de mando se encuentran listos."
            )
        else:
            text = (
                "📊 *Informe de Estado del Sistema:*\n\n"
                "• *Servidor Central:* 🟢 Activo (24/7)\n"
                "• *Estación Local (Laptop):* 🔴 Fuera de línea (Apagada o suspendida)\n\n"
                "Las consultas cognitivas y análisis funcionan normalmente en la nube. "
                "Para controlar su equipo, inicie el puente en su laptop."
            )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    async def cmd_abrir(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        if not context.args:
            await update.message.reply_text("Señor, indique qué desea abrir. Ej: `/abrir chrome` o `/abrir spotify`", parse_mode=ParseMode.MARKDOWN)
            return
        target = " ".join(context.args)
        await update.message.reply_text(f"⏳ Transmitiendo orden a su laptop: `{target}`...", parse_mode=ParseMode.MARKDOWN)
        res = await self.bridge.execute_action("open_item", {"target": target})
        if res.get("success"):
            await update.message.reply_text(f"✅ Ejecutado en su laptop: `{target}`", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(f"⚠️ No fue posible ejecutar la acción: {res.get('error')}")

    async def cmd_captura(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        if not self.bridge.is_connected:
            await update.message.reply_text("🔴 Señor, su laptop está fuera de línea. No es posible capturar la pantalla.")
            return
        await update.message.reply_text("📸 Capturando pantalla de su laptop...")
        res = await self.bridge.execute_action("screenshot", timeout=12.0)
        if res.get("success") and res.get("image_base64"):
            try:
                img_bytes = base64.b64decode(res["image_base64"])
                await update.message.reply_photo(
                    photo=io.BytesIO(img_bytes),
                    caption=f"🖥️ Captura de pantalla de su equipo ({res.get('timestamp', '')})",
                )
                return
            except Exception as e:
                await update.message.reply_text(f"⚠️ Error procesando imagen: {e}")
                return
        await update.message.reply_text(f"⚠️ No se pudo obtener la captura: {res.get('error')}")

    async def cmd_bloquear(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        res = await self.bridge.execute_action("lock_workstation")
        if res.get("success"):
            await update.message.reply_text("🔒 Estación de trabajo bloqueada con éxito.")
        else:
            await update.message.reply_text(f"⚠️ No se pudo bloquear la estación: {res.get('error')}")

    async def cmd_cmd(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        if not context.args:
            await update.message.reply_text("Indique el comando. Ej: `/cmd ipconfig`", parse_mode=ParseMode.MARKDOWN)
            return
        cmd_str = " ".join(context.args)
        await update.message.reply_text(f"⚙️ Ejecutando en laptop: `{cmd_str}`...", parse_mode=ParseMode.MARKDOWN)
        res = await self.bridge.execute_action("run_command", {"command": cmd_str}, timeout=30.0)
        if res.get("success"):
            stdout = res.get("stdout", "").strip() or "(Comando ejecutado sin salida en consola)"
            if len(stdout) > 3500:
                stdout = stdout[:3500] + "\n...(truncado)"
            await update.message.reply_text(f"```\n{stdout}\n```", parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(f"⚠️ Error en consola:\n```\n{res.get('error')}\n```", parse_mode=ParseMode.MARKDOWN)

    async def cmd_archivos(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        path = " ".join(context.args) if context.args else ""
        res = await self.bridge.execute_action("list_files", {"path": path})
        if res.get("success"):
            files = res.get("files", [])
            lines = [f"📂 *Directorio:* `{res.get('path')}`\n"]
            for f in files[:25]:
                icono = "📁" if f.get("is_dir") else "📄"
                lines.append(f"{icono} {f.get('name')}")
            if len(files) > 25:
                lines.append(f"\n_... y {len(files) - 25} elementos más._")
            await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(f"⚠️ Error listando archivos: {res.get('error')}")

    async def cmd_reset(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        uid = update.effective_user.id
        self.history[uid].clear()
        await update.message.reply_text("🧹 Memoria conversacional restablecida a los parámetros iniciales, Señor.")

    # -- Procesamiento de mensajes de texto y voz --
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.auth_check(update):
            return
        user_text = update.message.text
        uid = update.effective_user.id

        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

        # Agregar al historial
        self.history[uid].append({"role": "user", "text": user_text})
        if len(self.history[uid]) > MAX_HISTORY_LENGTH:
            self.history[uid] = self.history[uid][-MAX_HISTORY_LENGTH:]

        try:
            response_text = await self.gemini.generate(self.history[uid])
        except Exception as exc:
            logger.error("Error en Gemini: %s", exc)
            response_text = f"Lamento informarle, Señor, que ocurrió una anomalía cognitiva: {exc}"

        # Detectar y ejecutar posibles BRIDGE_ACTION
        matches = BRIDGE_ACTION_PATTERN.findall(response_text)
        for m in matches:
            try:
                action_data = json.loads(m)
                act = action_data.get("action", "")
                params = action_data.get("params", {})
                logger.info("Directiva de Gemini para la laptop detectada: %s", act)
                res = await self.bridge.execute_action(act, params)
                res_formatted = json.dumps(res, ensure_ascii=False, indent=2)
                response_text += f"\n\n📡 *Resultado en su Laptop (`{act}`):*\n```json\n{res_formatted}\n```"
            except Exception as e:
                response_text += f"\n\n⚠️ Error ejecutando acción `{act}`: {e}"

        self.history[uid].append({"role": "model", "text": response_text})

        # Enviar respuesta protegiendo límites de caracteres
        try:
            await update.message.reply_text(response_text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await update.message.reply_text(response_text, parse_mode=None)

    # -- Manejador del Gateway WebSocket para la Laptop --
    async def ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=25.0)
        await ws.prepare(request)

        token = request.headers.get("X-Bridge-Key") or request.query.get("token")
        if token != self.config.bridge_secret:
            logger.warning("Conexión rechazada: clave secreta incorrecta.")
            await ws.close(code=web.WSCloseCode.POLICY_VIOLATION)
            return ws

        # Esperar mensaje de handshake con información de la laptop
        try:
            init_msg = await ws.receive(timeout=10.0)
            if init_msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(init_msg.data)
                client_info = data.get("client_info", {})
                await self.bridge.register_connection(ws, client_info)
                # Confirmar conexión a la laptop
                await ws.send_json({"type": "handshake_ack", "server": "JARVIS Cloud Server 24/7"})
            else:
                await ws.close()
                return ws
        except Exception as exc:
            logger.error("Fallo durante el handshake del puente: %s", exc)
            await ws.close()
            return ws

        # Loop principal de escucha de la laptop
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self.bridge.handle_incoming_message(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        finally:
            await self.bridge.unregister_connection()

        return ws


async def main():
    config = ServerConfig.from_env()
    server = JarvisServer(config)

    # Inicializar Telegram
    tg_app = Application.builder().token(config.bot_token).build()
    server.tg_app = tg_app
    server.bridge.set_telegram_context(tg_app.bot, config.authorized_user_id)

    tg_app.add_handler(CommandHandler("start", server.cmd_start))
    tg_app.add_handler(CommandHandler("status", server.cmd_status))
    tg_app.add_handler(CommandHandler("abrir", server.cmd_abrir))
    tg_app.add_handler(CommandHandler("captura", server.cmd_captura))
    tg_app.add_handler(CommandHandler("screenshot", server.cmd_captura))
    tg_app.add_handler(CommandHandler("bloquear", server.cmd_bloquear))
    tg_app.add_handler(CommandHandler("cmd", server.cmd_cmd))
    tg_app.add_handler(CommandHandler("archivos", server.cmd_archivos))
    tg_app.add_handler(CommandHandler("reset", server.cmd_reset))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, server.handle_message))

    await tg_app.initialize()
    await tg_app.start()
    await tg_app.updater.start_polling(drop_pending_updates=True)

    # Iniciar Gateway HTTP / WebSocket
    app = web.Application()
    app.router.add_get("/ws/bridge", server.ws_handler)
    app.router.add_get("/", lambda r: web.Response(text="J.A.R.V.I.S. Cloud Gateway Online 24/7"))
    app.router.add_get("/health", lambda r: web.json_response({
        "status": "ok",
        "laptop_connected": server.bridge.is_connected,
        "service": "JARVIS Cloud"
    }))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.port)
    await site.start()

    logger.info("=" * 60)
    logger.info("🚀 J.A.R.V.I.S. Cloud Server iniciado con éxito en el puerto %d", config.port)
    logger.info("📡 Bot de Telegram escuchando mensajes...")
    logger.info("⏳ Esperando enlace de la estación de trabajo local en /ws/bridge...")
    logger.info("=" * 60)

    try:
        await asyncio.Event().wait()
    finally:
        await tg_app.updater.stop()
        await tg_app.stop()
        await tg_app.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Servidor detenido limpiamente.")
