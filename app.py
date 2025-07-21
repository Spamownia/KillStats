import re
import csv
import statistics
import requests
from collections import defaultdict
from ftplib import FTP
from io import BytesIO
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# --- KONFIGURACJA FTP ---
FTP_IP = "176.57.174.10"
FTP_PORT = 50021
FTP_USER = "gpftp37275281717442833"
FTP_PASS = "LXNdGShY"
FTP_PATH = "/SCUM/Saved/SaveFiles/Logs"

# --- WEBHOOKI DISCORD ---
WEBHOOK_TABLE1 = "https://discord.com/api/webhooks/1396229686475886704/Mp3CbZdHEob4tqsPSvxWJfZ63-Ao9admHCvX__XdT5c-mjYxizc7tEvb08xigXI5mVy3"
WEBHOOK_TABLE2 = WEBHOOK_TABLE1
WEBHOOK_TABLE3 = WEBHOOK_TABLE1

def send_discord(content, webhook_url):
    try:
        requests.post(webhook_url, json={"content": content}, timeout=10)
    except Exception as e:
        print(f"[ERROR] Wysyłka na Discord nie powiodła się: {e}")

def fetch_and_process_logs():
    try:
        ftp = FTP()
        ftp.connect(FTP_IP, FTP_PORT, timeout=30)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_PATH)

        log_files = []
        try:
            ftp.retrlines("MLSD", lambda line: log_files.append(line.split(";")[-1].strip()))
        except Exception as e:
            print(f"[ERROR] Nie udało się pobrać listy plików: {e}")
            ftp.quit()
            return

        log_files = [f for f in log_files if f.startswith("gameplay_") and f.endswith(".log")]
        if not log_files:
            print("[ERROR] Brak plików gameplay_*.log na FTP.")
            ftp.quit()
            return

        latest_log = sorted(log_files)[-1]

        log_text = ""
        with BytesIO() as bio:
            ftp.retrbinary(f"RETR {latest_log}", bio.write)
            log_text = bio.getvalue().decode("utf-16-le", errors="ignore")

        ftp.quit()

        pattern = re.compile(
            r"User: (?P<nick>[\w\d]+) \(\d+, [\d]+\)\. "
            r"Success: (?P<success>Yes|No)\. "
            r"Elapsed time: (?P<elapsed>[\d\.]+)\. "
            r"Failed attempts: (?P<failed_attempts>\d+)\. "
            r"Target object: [^\)]+\)\. "
            r"Lock type: (?P<lock_type>\w+)\."
        )

        data = {}
        user_lock_times = defaultdict(lambda: defaultdict(list))

        for match in pattern.finditer(log_text):
            nick = match.group("nick")
            lock_type = match.group("lock_type")
            success = match.group("success")
            failed_attempts = int(match.group("failed_attempts"))
            elapsed = float(match.group("elapsed"))

            key = (nick, lock_type)
            if key not in data:
                data[key] = {
                    "all_attempts": 0,
                    "successful_attempts": 0,
                    "failed_attempts": 0,
                    "times": [],
                }

            data[key]["all_attempts"] += 1
            if success == "Yes":
                data[key]["successful_attempts"] += 1
            else:
                data[key]["failed_attempts"] += 1

            data[key]["times"].append(elapsed)
            user_lock_times[nick][lock_type].append(elapsed)

        lock_order = {"VeryEasy": 0, "Basic": 1, "Medium": 2, "Advanced": 3, "DialLock": 4}

        sorted_data = sorted(
            data.items(),
            key=lambda x: (x[0][0], lock_order.get(x[0][1], 99))
        )

        # --- Tabela główna ---
        csv_rows = []
        last_nick = None
        for (nick, lock_type), stats in sorted_data:
            if last_nick and nick != last_nick:
                csv_rows.append([""] * 7)
            last_nick = nick

            all_attempts = stats["all_attempts"]
            successful_attempts = stats["successful_attempts"]
            failed_attempts = stats["failed_attempts"]
            avg_time = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
            effectiveness = round(100 * successful_attempts / all_attempts, 2) if all_attempts else 0

            csv_rows.append([
                nick, lock_type, all_attempts, successful_attempts, failed_attempts,
                f"{effectiveness}%", f"{avg_time}s"
            ])

        # --- Wysyłka tabeli głównej ---
        table_block = "```\n"
        table_block += f"{'Nick':<10} {'Zamek':<10} {'Wszystkie':<12} {'Udane':<6} {'Nieudane':<9} {'Skut.':<8} {'Śr. czas':<8}\n"
        table_block += "-" * 70 + "\n"
        for row in csv_rows:
            if any(row):
                table_block += f"{row[0]:<10} {row[1]:<10} {str(row[2]):<12} {str(row[3]):<6} {str(row[4]):<9} {row[5]:<8} {row[6]:<8}\n"
            else:
                table_block += "\n"
        table_block += "```"
        send_discord(table_block, WEBHOOK_TABLE1)

        # --- Tabela admin ---
        admin_csv_rows = [["Nick", "Rodzaj zamka", "Skuteczność", "Średni czas"]]
        last_nick_admin = None
        for (nick, lock_type), stats in sorted_data:
            if last_nick_admin and nick != last_nick_admin:
                admin_csv_rows.append([""] * 4)
            last_nick_admin = nick

            all_attempts = stats["all_attempts"]
            succ = stats["successful_attempts"]
            eff = round(100 * succ / all_attempts, 2) if all_attempts else 0
            avg = round(statistics.mean(stats["times"]), 2) if stats["times"] else 0
            admin_csv_rows.append([nick, lock_type, f"{eff}%", f"{avg}s"])

        summary_block = "```\n"
        summary_block += f"{'Nick':<10} {'Zamek':<10} {'Skut.':<10} {'Śr. czas':<10}\n"
        summary_block += "-" * 45 + "\n"
        for row in admin_csv_rows[1:]:
            if any(row):
                summary_block += f"{row[0]:<10} {row[1]:<10} {row[2]:<10} {row[3]:<10}\n"
            else:
                summary_block += "\n"
        summary_block += "```"
        send_discord(summary_block, WEBHOOK_TABLE2)

        # --- Podium ---
        ranking = []
        for nick in user_lock_times:
            times_all = [t for lock in user_lock_times[nick].values() for t in lock]
            total_attempts = len(times_all)
            total_success = sum(1 for lock in user_lock_times[nick].values() for _ in lock)  # uproszczone
            effectiveness = round(100 * total_success / total_attempts, 2) if total_attempts else 0
            avg_time = round(statistics.mean(times_all), 2) if total_attempts else 0
            ranking.append((nick, effectiveness, avg_time))

        ranking = sorted(ranking, key=lambda x: (-x[1], x[2]))[:5]

        col_widths = [10, 14, 14, 14]
        podium_block = "```\n"
        podium_block += "                     🏆 PODIUM           \n"
        podium_block += "-" * sum(col_widths) + "\n"
        podium_block += f"{'Miejsce':^{col_widths[0]}}{'Nick':^{col_widths[1]}}{'Skuteczność':^{col_widths[2]}}{'Średni czas':^{col_widths[3]}}\n"

        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]

        for i, (nick, eff, avg) in enumerate(ranking):
            medal = medals[i]
            place = f"{i+1}"
            podium_block += f"{medal:<2}{place:^{col_widths[0]-2}}{nick:^{col_widths[1]}}{str(eff)+'%':^{col_widths[2]}}{str(avg)+' s':^{col_widths[3]}}\n"

        podium_block += "```"
        send_discord(podium_block, WEBHOOK_TABLE3)

        print("[INFO] Przetwarzanie i wysyłka zakończone sukcesem.")
    except Exception as e:
        print(f"[ERROR] Błąd podczas przetwarzania logów: {e}")

# Uruchomienie harmonogramu
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_and_process_logs, 'interval', seconds=60)
scheduler.start()

@app.route("/")
def index():
    return "KillStats działa poprawnie"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
