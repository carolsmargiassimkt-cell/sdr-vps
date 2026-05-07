from datetime import datetime
try:
    import holidays
except Exception:
    raise SystemExit(0)  # se não tiver lib, não bloqueia

today = datetime.now().date()
br = holidays.Brazil()
raise SystemExit(1 if today in br or today.weekday() >= 5 else 0)
