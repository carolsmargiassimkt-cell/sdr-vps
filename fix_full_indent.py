import re

path = "/root/sdr-vps/supervisor.py"

with open(path, "r", encoding="utf-8", errors="ignore") as f:
    lines = f.readlines()

fixed = []
indent_level = 0

for line in lines:
    stripped = line.strip()

    # pula linhas vazias
    if not stripped:
        fixed.append("\n")
        continue

    # reduz indentação para blocos de fechamento
    if re.match(r'^(except|elif|else|finally)\b', stripped):
        indent_level = max(indent_level - 1, 0)

    # aplica indentação
    fixed.append("    " * indent_level + stripped + "\n")

    # aumenta indentação após blocos
    if stripped.endswith(":"):
        indent_level += 1

with open(path, "w", encoding="utf-8") as f:
    f.writelines(fixed)

print("[OK] indentação reconstruída")
