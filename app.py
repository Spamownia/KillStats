import os
import ftplib
import re
import psycopg
import requests
from datetime import datetime
from psycopg import sql

# --- KONFIGURACJA FTP ---
FTP_HOST = "195.179.226.218"
FTP_PORT = 56421
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs/"

# --- KONFIGURACJA BAZY NEON ---
DB_HOST = 'ep-hidden-band-a2ir2x2r-pooler.eu-central-1.aws.neon.tech'
DB_NAME = 'neondb'
DB_USER = 'neondb_owner'
DB_PASS = 'npg_dRU1YCtxbh6v'
DB_PORT = 5432
DB_SSLMODE = 'require'

# --- KONFIGURACJA WEBHOOKA DISCORD ---
WEBHOOK_URL = "https://discord.com/api/webhooks/1385204325235691633/0Dey6Ywk_mDiZaBYh4cCCbuGAU5fPuLqcSpWVRkDxhVK-KIjGfzsXKkChDmAUoSrhv3R"

# --- TWORZENIE TABELI W BAZIE ---
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS killstats (
    data_czas TIMESTAMP NOT NULL,
    zabojca TEXT NOT NULL,
    ofiara TEXT NOT NULL,
    bron TEXT,
    PRIMARY KEY (data_czas, zabojca, ofiara)
);
"""

INSERT_SQL = """
INSERT INTO killstats (data_czas, zabojca, ofiara, bron)
VALUES (%s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

# --- FUNKCJA POBRANIA LISTY LOGÓW ---
def get_log_files():
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)

    files = []
    ftp.retrlines('LIST', lambda x: files.append(x.split()[-1]))
    ftp.quit()
    return [f for f in files if f.lower().startswith("kill_") and f.lower().endswith(".log")]

# --- POPRAWIONA FUNKCJA PARSOWANIA LOGU Z DEBUGAMI ---
def parse_log(filename):
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)

    log_data = []
    ftp.retrbinary(f"RETR {filename}", log_data.append)
    ftp.quit()

    content = b"".join(log_data).decode("utf-16-le", errors="ignore")

    pattern = re.compile(
        r"^(?P<data>\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}): Died: (?P<ofiara>.+?) \(\d+\), Killer: (?P<zabojca>.+?) \(\d+\) Weapon: (?P<bron>.+?) S:.*Distance: (?P<distance>[\d\.]+) m",
        re.MULTILINE
    )

    entries = []
    for match in pattern.finditer(content):
        data_czas_raw = match.group("data")
        ofiara = match.group("ofiara").strip()
        zabojca = match.group("zabojca").strip()
        bron = match.group("bron").strip()

        # Poprawione formatowanie daty dla czytelności i stabilności
        try:
            dt_str = data_czas_raw.replace('.', '-')
            dt_str = dt_str[:10] + ' ' + dt_str[11:].replace('.', ':')
            data_czas = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"[DEBUG][BŁĄD DATY] Nie udało się sparsować daty '{data_czas_raw}': {e}")
            continue

        entries.append((data_czas, zabojca, ofiara, bron))

    print(f"[DEBUG] W pliku {filename} znaleziono {len(entries)} wpisów pasujących do wzorca.")
    return entries

# --- FUNKCJA WYSYŁAJĄCA WEBHOOK (wyłącznie requests) ---
def send_discord_webhook(message: str):
    payload = {"content": message}
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code != 204:
            print(f"[WEBHOOK ERROR] Status: {resp.status_code} | Response: {resp.text}")
        else:
            print("[WEBHOOK] Wiadomość wysłana na Discord.")
    except Exception as e:
        print(f"[WEBHOOK ERROR] Wyjątek: {e}")

# --- GŁÓWNA FUNKCJA Z DEBUGAMI BAZY ---
def main():
    conn = psycopg.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        sslmode=DB_SSLMODE
    )
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    log_files = get_log_files()
    print(f"[INFO] Znaleziono {len(log_files)} plików logów.")

    total_new = 0
    for log_file in log_files:
        print(f"[INFO] Przetwarzam plik: {log_file}")
        entries = parse_log(log_file)
        new_entries = 0
        for entry in entries:
            try:
                cur.execute(INSERT_SQL, entry)
                print(f"[DB DEBUG] Próba wstawienia: {entry} | rowcount: {cur.rowcount}")
                if cur.rowcount == 1:
                    new_entries += 1
            except Exception as e:
                print(f"[DB ERROR] Nie udało się zapisać wpisu {entry}: {e}")
        total_new += new_entries
        print(f"[INFO] Nowych wpisów w tym pliku: {new_entries}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"[INFO] Przetwarzanie logów zakończone. Nowych wpisów łącznie: {total_new}")

    if total_new > 0:
        send_discord_webhook(f"Przetworzono logi SCUM. Nowych zabójstw zapisano: {total_new}")
    else:
        print("[INFO] Brak nowych wpisów do wysłania na Discord.")

if __name__ == "__main__":
    main()
