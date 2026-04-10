from __future__ import annotations

from crm.pipedrive_client import PipedriveClient


def main() -> int:
    crm = PipedriveClient()
    total = int(crm.cleanup_bot_call_activities_for_date() or 0)
    print(f"[CRM_LIMPEZA_ATIVIDADES] total={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
