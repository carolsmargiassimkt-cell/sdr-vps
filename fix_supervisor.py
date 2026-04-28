
import re
import os

def extract_lines_between(lines, start_prefix, end_prefix=None):
    start_idx = -1
    for i, line in enumerate(lines):
        if line.startswith(start_prefix):
            start_idx = i
            break
    if start_idx == -1:
        return []
    
    if end_prefix is None:
        return lines[start_idx:]
    
    end_idx = -1
    for i in range(start_idx + 1, len(lines)):
        if lines[i].startswith(end_prefix):
            end_idx = i
            break
    if end_idx == -1:
        return lines[start_idx:]
    return lines[start_idx:end_idx]

def get_imports(lines):
    return [l.strip() for l in lines if l.startswith('import ') or l.startswith('from ')]

def get_constants(lines):
    constants = []
    current_const = None
    for line in lines:
        if re.match(r'^[A-Z_][A-Z0-9_]*\s*=', line):
            if current_const:
                constants.append(current_const)
            current_const = line
        elif current_const and line.strip() and not re.match(r'^(def |class |import |from |if )', line):
             # Heuristic for multi-line constants: continuation if indented or starts with certain chars
             if line.startswith(' ') or line.startswith('\t') or any(line.strip().startswith(c) for c in '{["' + "'"):
                 current_const += line
             else:
                 # Check if the previous line was unfinished (e.g. ended with { or ,)
                 stripped_prev = current_const.strip()
                 if stripped_prev.endswith('{') or stripped_prev.endswith('[') or stripped_prev.endswith('(') or stripped_prev.endswith(','):
                     current_const += line
                 else:
                     constants.append(current_const)
                     current_const = None
        else:
            if current_const:
                constants.append(current_const)
                current_const = None
    if current_const:
        constants.append(current_const)
    return constants

def get_functions(lines):
    funcs = {}
    current_func = None
    current_body = []
    for line in lines:
        if line.startswith('def '):
            if current_func:
                funcs[current_func] = current_body
            current_func = line.split('(')[0].split('def ')[1].strip()
            current_body = [line]
        elif (line.startswith('class ') or line.startswith('if __name__')) and current_func:
            funcs[current_func] = current_body
            current_func = None
            current_body = []
        elif current_func:
            current_body.append(line)
    if current_func:
        funcs[current_func] = current_body
    return funcs

def main():
    with open('/root/sdr-vps/supervisor.py', 'r', encoding='utf-8') as f:
        cur_lines = f.readlines()
    with open('/root/sdr-vps/supervisor.py.bak', 'r', encoding='utf-8') as f:
        bak_lines = f.readlines()

    # 1. Imports
    imports = set(get_imports(bak_lines) + get_imports(cur_lines))
    
    # 2. Constants
    constants = {}
    for c in get_constants(bak_lines) + get_constants(cur_lines):
        name = c.split('=')[0].strip()
        constants[name] = c
    
    if 'SUPER_MINAS_REENTRY' not in constants:
        constants['SUPER_MINAS_REENTRY'] = 'SUPER_MINAS_REENTRY = True\n'

    # 3. Path logic and special blocks
    path_logic = extract_lines_between(bak_lines, 'ROOT_DIR =', 'try:')
    if not path_logic: path_logic = extract_lines_between(cur_lines, 'ROOT_DIR =', 'try:')
    
    logic_imports = extract_lines_between(cur_lines, 'try:', 'def _pip_install')
    if not logic_imports: logic_imports = extract_lines_between(bak_lines, 'try:', 'def _pip_install')

    # 4. Functions
    bak_funcs = get_functions(bak_lines)
    cur_funcs = get_functions(cur_lines)
    
    # Known broken in cur
    broken = ['get_whatsapp_status', 'spawn_if_needed', 'can_send_outbound', 'is_within_business_hours', 'is_business_hours', 'cleanup_children', 'handle_exit', '_today_str']
    
    merged_funcs = {}
    # Start with bak
    for name, body in bak_funcs.items():
        merged_funcs[name] = body
    # Add/Update from cur if not broken
    for name, body in cur_funcs.items():
        if name not in merged_funcs or name not in broken:
             if name not in merged_funcs:
                 merged_funcs[name] = body
    
    # 5. Class and Main
    class_lines = extract_lines_between(cur_lines, 'class SDRSupervisor:', 'if __name__ == "__main__":')
    main_lines = extract_lines_between(cur_lines, 'if __name__ == "__main__":')

    # Reconstruct
    out = []
    for imp in sorted(list(imports)):
        out.append(imp + "\n")
    out.append("\n")
    out.extend(path_logic)
    out.append("\n")
    out.extend(logic_imports)
    out.append("\n")
    for name in sorted(constants.keys()):
        val = constants[name]
        if not val.endswith('\n'): val += '\n'
        out.append(val)
    out.append("\n")
    for name in sorted(merged_funcs.keys()):
        out.extend(merged_funcs[name])
        out.append("\n")
    
    out.append("\n")
    out.extend(class_lines)
    out.append("\n")
    out.extend(main_lines)

    # Clean up duplicate imports at the top
    unique_out = []
    seen = set()
    for line in out:
        if (line.startswith('import ') or line.startswith('from ')) and line.strip() in seen:
            continue
        if line.startswith('import ') or line.startswith('from '):
            seen.add(line.strip())
        unique_out.append(line)

    with open('/root/sdr-vps/supervisor_fixed.py', 'w', encoding='utf-8') as f:
        f.writelines(unique_out)

if __name__ == "__main__":
    main()
