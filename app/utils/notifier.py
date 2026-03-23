"""Send email notification after execution."""
from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

from app.config import settings


def send_completion_email(
    year: int,
    week: int,
    total: int,
    success_count: int,
    failure_count: int,
    failed_providers: list[str],
    homologation_path: Path | None,
    logger: logging.Logger | None = None,
) -> None:
    """Send summary email if SMTP is configured."""
    if not settings.NOTIFY_EMAIL or not settings.SMTP_HOST:
        return

    subject = f"[RPA] S{week:02d}/{year} — {success_count}/{total} OK"

    lines = [
        f"Ejecución semana S{week:02d}/{year} completada.",
        "",
        f"  ✓ Exitosos : {success_count}/{total}",
        f"  ✗ Fallidos : {failure_count}/{total}",
    ]
    if failed_providers:
        lines.append("")
        lines.append("Proveedores fallidos:")
        for name in failed_providers:
            lines.append(f"  - {name}")
    if homologation_path:
        lines.append("")
        lines.append(f"Homologación guardada en:")
        lines.append(f"  {homologation_path}")

    body = "\n".join(lines)

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = settings.SMTP_USER or settings.NOTIFY_EMAIL
    msg["To"] = settings.NOTIFY_EMAIL
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as server:
            server.ehlo()
            if settings.SMTP_TLS:
                server.starttls()
                server.ehlo()
            if settings.SMTP_USER and settings.SMTP_PASS:
                server.login(settings.SMTP_USER, settings.SMTP_PASS)
            server.sendmail(msg["From"], [settings.NOTIFY_EMAIL], msg.as_string())
        if logger:
            logger.info("[Notificación] Email enviado a %s", settings.NOTIFY_EMAIL)
    except Exception as exc:
        if logger:
            logger.warning("[Notificación] No se pudo enviar email: %s", exc)
