import csv
from collections import Counter
import os
import re
import requests
import time
import threading
from ftplib import FTP
from flask import Flask

# FTP config
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
LOG_DIR = "/SCUM/Saved/SaveFiles/Logs"

WEBHOOK_URL = "https://discord.com/api/webhooks/1396497733984059472/ie6Hk_yTKETBHBriA9aCP0IbWJbqwXeskGiAdMyP2RMy_ww1Z2h2UCaw4jTbbOJ_e3gO"  # Podmień na swój webhook

def get_file_list(ftp):
    files = []

    def parse_line(line):
        parts = line.split(maxsplit=8)
        if len(parts) == 9:
            filename = parts[8]
            files.append(filename)

    ftp.retrlines('LIST', callback=parse_line)
    return files

def get_latest_log_from_ftp():
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(LOG_DIR)

    files = get_file_list(ftp)
    kill_logs = sorted([f for f in files if f.startswith("kill_") and f.endswith(".log")])
    if not kill_logs:
        ftp.quit()
        return None

    latest_log = kill_logs[-1]
    print(f"[FTP] Ostatni log: {latest_log}")

    local_log = "latest_kill.log"
    with open(local_log, "wb") as f:
        ftp.retrbinary(f"RETR {latest_log}", f.write)

    ftp.quit()
    return local_log

def read_new_lines(log_file, cache_file="last_log_line.txt"):
    if not os.path.exists(cache_file):
        open(cache_file, 'w').close()

    with open(cache_file, "r") as f:
        last_line = f.read().strip()

    with open(log_file, "r", encoding="utf-16-le", errors="ignore") as f:
        lines = f.readlines()

    new_lines = []
    found_last = last_line == ""
    for line in lines:
        line = line.strip()
        if found_last:
            new_lines.append(line)
        elif line == last_line:
            found_last = True

    if lines:
        with open(cache_file, "w") as f:
            f.write(lines[-1].strip())

    return new_lines

def parse_and_update_kills(log_lines, csv_file="kills.csv"):
    if os.path.exists(csv_file):
        with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            last_count = int(rows[-1]["suma_zabojstw"]) if rows else 0
    else:
        last_count = 0

    file_exists = os.path.isfile(csv_file)
    fieldnames = ["data_czas", "zabojca", "ofiara", "bron", "odleglosc", "suma_zabojstw"]

    with open(csv_file, mode='a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for line in log_lines:
            if "Died:" in line:
                match = re.search(r'^(.*?)\: Died: (.*?) \(\d+\), Killer: (.*?) \(\d+\) Weapon: (.*?) S:.*Distance: ([\d\.]+) m', line)
                if match:
                    data_czas = match.group(1).replace('.', '-').replace('-', ' ', 2).replace('-', ':', 1)
                    ofiara = match.group(2)
                    zabojca = match.group(3)
                    bron = match.group(4).strip()
                    odleglosc = match.group(5).strip()

                    last_count += 1

                    writer.writerow({
                        "data_czas": data_czas,
                        "zabojca": zabojca,
                        "ofiara": ofiara,
                        "bron": bron,
                        "odleglosc": odleglosc,
                        "suma_zabojstw": last_count
                    })
                    print(f"[OK] Zapisano: {data_czas}, {zabojca}, {ofiara}, {bron}, {odleglosc}m, suma: {last_count}")

def generate_podium(csv_file="kills.csv", podium_file="podium.csv"):
    kills_count = Counter()
    with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            kills_count[row["zabojca"]] += 1

    sorted_kills = kills_count.most_common()
    podium_rows = []
    medale = ["🥇", "🥈", "🥉"]

    for i, (nick, ilosc) in enumerate(sorted_kills, start=1):
        medal = medale[i-1] if i <= 3 else ""
        podium_rows.append({
            "medal": medal,
            "Miejsce": str(i),
            "Nick": nick,
            "Suma Zabójstw": str(ilosc)
        })

    fieldnames = ["medal", "Miejsce", "Nick", "Suma Zabójstw"]
    col_widths = {field: len(field) for field in fieldnames}
    for row in podium_rows:
        for field in fieldnames:
            col_widths[field] = max(col_widths[field], len(row.get(field, "")))

    lines = ["🏆RANKING🏆", ""]
    header = " | ".join(f"{field.center(col_widths[field])}" if field != "medal" else " ".center(col_widths[field]) for field in fieldnames)
    separator = "-+-".join('-'*col_widths[field] for field in fieldnames)
    lines.append(header)
    lines.append(separator)
    for row in podium_rows:
        line = " | ".join(f"{row.get(field,'').center(col_widths[field])}" for field in fieldnames)
        lines.append(line)

    table_text = "```\n" + "\n".join(lines) + "\n```"

    with open(podium_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in podium_rows:
            writer.writerow(row)

    print(f"[OK] Podium zapisane do {podium_file}")

    response = requests.post(WEBHOOK_URL, json={"content": table_text})
    if response.status_code == 204:
        print("[OK] Tabela podium wysłana na Discord.")
    else:
        print(f"[BŁĄD] Nie udało się wysłać na Discord: {response.status_code} {response.text}")

def main_loop():
    print("[START] Skrypt uruchomiony. Sprawdzanie co 60 sekund...")
    while True:
        try:
            latest_log_file = get_latest_log_from_ftp()
            if not latest_log_file:
                print("[FTP] Brak plików kill_*.log")
            else:
                new_log_lines = read_new_lines(latest_log_file)
                if new_log_lines:
                    print(f"[INFO] Znaleziono {len(new_log_lines)} nowych wpisów w logu.")
                    parse_and_update_kills(new_log_lines)
                    generate_podium()
                else:
                    print("[INFO] Brak nowych wpisów w logu.")
        except Exception as e:
            print(f"[BŁĄD] Wyjątek: {e}")

        time.sleep(60)

# 🔷 Flask server setup
app = Flask(__name__)

@app.route('/')
def index():
    return "Alive"

if __name__ == "__main__":
    # Uruchom główną pętlę w osobnym wątku
    t = threading.Thread(target=main_loop, daemon=True)
    t.start()

    # Uruchom serwer Flask na 0.0.0.0:10000
    app.run(host='0.0.0.0', port=10000)
