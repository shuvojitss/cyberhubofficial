import os
import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional

try:
  from openai import OpenAI
except Exception:  # pragma: no cover - optional dependency fallback
  OpenAI = None


NIM_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_MODEL = os.getenv("AI_ASSISTANT_MODEL", "openai/gpt-oss-120b")


def _get_nvidia_api_key() -> str:
  return os.getenv("NVIDIA_API_KEY", "").strip()


def _extract_text(message_obj: Any) -> str:
  content = getattr(message_obj, "content", "")
  if isinstance(content, str):
    return content
  if isinstance(content, list):
    parts = []
    for part in content:
      if isinstance(part, str):
        parts.append(part)
      elif isinstance(part, dict):
        parts.append(str(part.get("text", "")))
      else:
        parts.append(str(getattr(part, "text", "")))
    return "".join(parts)
  return ""


def _format_value(value: Any) -> str:
  if value is None:
    return ""
  text = str(value).strip()
  return text


def _normalise_text(value: Any) -> str:
  return re.sub(r"\s+", " ", _format_value(value).lower()).strip()


def _last_user_message(messages: Iterable[Dict[str, Any]]) -> str:
  for message in reversed(list(messages)):
    role = str(message.get("role", "")).lower()
    if role == "user":
      return _format_value(message.get("content", ""))
  return ""


def _count_by(alerts: List[Dict[str, str]], key: str) -> List[Dict[str, Any]]:
  counter = Counter(alert.get(key, "Unknown") or "Unknown" for alert in alerts)
  return [{"value": value, "count": count} for value, count in counter.most_common()]


def _build_summary(alerts: List[Dict[str, str]]) -> Dict[str, Any]:
  severity_counter = Counter((alert.get("severity") or "medium").lower() for alert in alerts)
  status_counter = Counter((alert.get("status") or "active").lower() for alert in alerts)
  total = len(alerts)
  critical_alerts = [alert for alert in alerts if (alert.get("severity") or "").lower() == "critical"]
  high_alerts = [alert for alert in alerts if (alert.get("severity") or "").lower() == "high"]

  return {
    "total": total,
    "severity_counts": {
      "critical": severity_counter.get("critical", 0),
      "high": severity_counter.get("high", 0),
      "medium": severity_counter.get("medium", 0),
      "low": severity_counter.get("low", 0),
    },
    "status_counts": dict(status_counter),
    "critical_alerts": critical_alerts[:5],
    "high_alerts": high_alerts[:5],
    "top_sources": _count_by(alerts, "source")[:5],
    "top_targets": _count_by(alerts, "target")[:5],
    "recent_alerts": list(reversed(alerts[-5:])),
  }


def _build_context_text(summary: Dict[str, Any]) -> str:
  lines = [
    f"Total alerts: {summary['total']}",
    "Severity counts:",
    f"- Critical: {summary['severity_counts']['critical']}",
    f"- High: {summary['severity_counts']['high']}",
    f"- Medium: {summary['severity_counts']['medium']}",
    f"- Low: {summary['severity_counts']['low']}",
  ]

  if summary["status_counts"]:
    status_text = ", ".join(f"{key}: {value}" for key, value in summary["status_counts"].items())
    lines.append(f"Status counts: {status_text}")

  if summary["critical_alerts"]:
    lines.append("Critical alerts:")
    for alert in summary["critical_alerts"]:
      lines.append(
        f"- {alert.get('id', '-')}: {alert.get('name', 'Unknown')} | {alert.get('source', '-')} -> {alert.get('target', '-')} | {alert.get('detectedAt', '-')}"
      )

  if summary["high_alerts"]:
    lines.append("High severity alerts:")
    for alert in summary["high_alerts"]:
      lines.append(
        f"- {alert.get('id', '-')}: {alert.get('name', 'Unknown')} | {alert.get('source', '-')} -> {alert.get('target', '-')} | {alert.get('detectedAt', '-')}"
      )

  if summary["top_sources"]:
    lines.append(
      "Top sources: "
      + ", ".join(f"{item['value']} ({item['count']})" for item in summary["top_sources"])
    )
  if summary["top_targets"]:
    lines.append(
      "Top targets: "
      + ", ".join(f"{item['value']} ({item['count']})" for item in summary["top_targets"])
    )

  if summary["recent_alerts"]:
    lines.append("Recent alerts:")
    for alert in summary["recent_alerts"]:
      lines.append(
        f"- {alert.get('id', '-')}: {alert.get('name', 'Unknown')} | {alert.get('severity', 'medium')} | {alert.get('status', 'active')}"
      )

  return "\n".join(lines)


def _build_fallback_reply(question: str, summary: Dict[str, Any]) -> str:
  if summary["total"] == 0:
    return "I could not find any alerts in alerts.xlsx, so there is no data to analyse yet."

  question_text = question.lower()
  severity = summary["severity_counts"]
  critical = severity["critical"]
  high = severity["high"]

  if any(token in question_text for token in ["critical", "highest", "severe"]):
    if summary["critical_alerts"]:
      alert_lines = []
      for alert in summary["critical_alerts"]:
        alert_lines.append(
          f"- {alert.get('id', '-')}: {alert.get('name', 'Unknown')} from {alert.get('source', '-')} to {alert.get('target', '-')} at {alert.get('detectedAt', '-')}."
        )
      return (
        f"There are {critical} critical alerts in the workbook and {high} high-severity alerts.\n"
        + "Most urgent items:\n"
        + "\n".join(alert_lines)
        + "\nRecommended action: isolate the affected targets, confirm the source IPs, and review the status of each alert."
      )

  if any(token in question_text for token in ["status", "status counts", "active", "resolved"]):
    status_text = ", ".join(f"{key}: {value}" for key, value in summary["status_counts"].items())
    return (
      f"The workbook contains {summary['total']} alerts. Current status distribution: {status_text or 'no status data found'}. "
      "Use the alerts dashboard to update a threat's status if investigation progress changes."
    )

  if any(token in question_text for token in ["source", "source ip", "origin"]):
    return (
      "The most common sources in the workbook are: "
      + ", ".join(f"{item['value']} ({item['count']})" for item in summary["top_sources"]) 
      + "."
    )

  if any(token in question_text for token in ["target", "destination", "asset"]):
    return (
      "The most frequently targeted assets are: "
      + ", ".join(f"{item['value']} ({item['count']})" for item in summary["top_targets"]) 
      + "."
    )

  return (
    f"I reviewed {summary['total']} alerts from alerts.xlsx. "
    f"There are {critical} critical, {high} high, {severity['medium']} medium, and {severity['low']} low severity alerts. "
    "The highest priority items are the critical alerts, which you should investigate first."
  )


def _build_suggestions(question: str, summary: Dict[str, Any]) -> List[str]:
  question_text = question.lower()
  if any(token in question_text for token in ["critical", "highest", "severe"]):
    return ["Show all critical alerts", "Summarize high-severity alerts", "Recommend mitigation steps", "List top targets"]
  if any(token in question_text for token in ["status", "resolved", "active"]):
    return ["Show status distribution", "List unresolved alerts", "Update an alert status", "Review recent alerts"]
  if any(token in question_text for token in ["source", "origin", "ip"]):
    return ["Show top sources", "Find repeated attackers", "List impacted targets", "Summarize recent alerts"]
  return ["Summarize critical alerts", "Show top sources", "Show top targets", "Review recent alerts"]


def build_assistant_reply(messages: List[Dict[str, Any]], alerts: List[Dict[str, str]]) -> Dict[str, Any]:
  question = _last_user_message(messages)
  summary = _build_summary(alerts)
  context_text = _build_context_text(summary)
  nvidia_api_key = _get_nvidia_api_key()

  if OpenAI is not None and nvidia_api_key:
    try:
      client = OpenAI(base_url=NIM_BASE_URL, api_key=nvidia_api_key)
      completion = client.chat.completions.create(
        model=DEFAULT_MODEL,
        messages=[
          {
            "role": "system",
            "content": (
              "You are a cybersecurity assistant. Answer only from the alert workbook context below. "
              "If the question asks for something outside the workbook, say that you cannot verify it from the file.\n\n"
              f"Workbook context:\n{context_text}"
            ),
          },
          *messages[-12:],
        ],
        temperature=0.2,
        top_p=0.9,
        max_tokens=700,
        stream=False,
      )

      choice = completion.choices[0] if completion.choices else None
      message = getattr(choice, "message", None)
      reply = _extract_text(message) if message else ""
      reasoning = ""
      if message is not None:
        reasoning = getattr(message, "reasoning_content", "") or ""

      if reply.strip():
        return {
          "reply": reply,
          "reasoning": reasoning,
          "model": DEFAULT_MODEL,
          "suggestions": _build_suggestions(question, summary),
          "alertCount": summary["total"],
        }
    except Exception as exc:
      fallback_reply = _build_fallback_reply(question, summary)
      return {
        "reply": fallback_reply,
        "reasoning": f"Falling back to local analysis: {exc}",
        "model": "local-fallback",
        "suggestions": _build_suggestions(question, summary),
        "alertCount": summary["total"],
      }

  return {
    "reply": _build_fallback_reply(question, summary),
    "reasoning": "Local analysis based on alerts.xlsx",
    "model": "local-fallback",
    "suggestions": _build_suggestions(question, summary),
    "alertCount": summary["total"],
  }

