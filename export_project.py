import os

ROOT = os.path.abspath(".")

# =========================
# EXCLUSIONS
# =========================
EXCLUDE_DIRS = {
    "node_modules",
    "logs",
    "scripts/logs",
    "scripts/__pycache__",
    ".git",
    ".vscode",
    "__pycache__",
    "dist",
    "build",
    "out",
    ".next",
    ".venv",
    ".cache"
}
EXCLUDE_FILES = {
    ".env",
    "ca.pem",
    "package-lock.json",
    ".env.local",
    ".env.production",
    ".env.development",
    ".tsbuildinfo",
    "index.js.map",
    "export_project.py",
    "devserver.sh"
    
}

EXCLUDE_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp",
    ".pdf", ".zip", ".tar", ".gz",
    ".exe", ".dll", ".so",".md",".log",
    ".lock", ".tsbuildinfo"
}

def is_hidden(path):
    return any(part.startswith(".") for part in path.split(os.sep))

# =========================
# TREE GENERATION (FIXED)
# =========================
with open("tree.md", "w", encoding="utf-8") as tree:
    tree.write("# 📁 Project Structure\n\n")

    for root, dirs, files in os.walk(ROOT):

        # Normalize path
        root = os.path.abspath(root)

        # Exclude hidden & unwanted dirs
        dirs[:] = [
            d for d in dirs
            if d not in EXCLUDE_DIRS and not d.startswith(".")
        ]

        if is_hidden(os.path.relpath(root, ROOT)) and root != ROOT:
            continue

        rel_path = os.path.relpath(root, ROOT)
        level = 0 if rel_path == "." else rel_path.count(os.sep) + 1
        indent = "  " * level

        folder_name = "algostrategy" if rel_path == "." else os.path.basename(root)
        tree.write(f"{indent}- 📁 {folder_name}/\n")

        for file in sorted(files):
            if (
                file.startswith(".")
                or file in EXCLUDE_FILES
                or os.path.splitext(file)[1] in EXCLUDE_EXTS
            ):
                continue

            tree.write(f"{indent}  - 📄 {file}\n")

# =========================
# CONTENTS GENERATION (FOLDER-AWARE)
# =========================
with open("contents.md", "w", encoding="utf-8") as contents:
    contents.write("# 📄 Project File Contents\n\n")

    for root, dirs, files in os.walk(ROOT):

        root = os.path.abspath(root)

        dirs[:] = [
            d for d in dirs
            if d not in EXCLUDE_DIRS and not d.startswith(".")
        ]

        if is_hidden(os.path.relpath(root, ROOT)) and root != ROOT:
            continue

        rel_dir = os.path.relpath(root, ROOT)
        folder = "algostrategy" if rel_dir == "." else rel_dir

        contents.write(f"## 📁 {folder}/\n\n")

        for file in sorted(files):
            if (
                file.startswith(".")
                or file in EXCLUDE_FILES
                or os.path.splitext(file)[1] in EXCLUDE_EXTS
            ):
                continue

            filepath = os.path.join(root, file)
            relpath = os.path.relpath(filepath, ROOT)
            ext = os.path.splitext(file)[1].replace(".", "")

            contents.write(f"### 📄 {relpath}\n\n")
            contents.write(f"```{ext}\n")

            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    contents.write(f.read())
            except Exception:
                contents.write("<<Binary or unreadable file>>")

            contents.write("\n```\n\n")
