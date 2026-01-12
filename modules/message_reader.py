from pathlib import Path
import shutil
import tempfile
import pandas as pd
import sqlite3

class MessageReader:
    def __init__(self, db_path: str = "~/Library/Messages/chat.db"):
        self.db_path = Path(db_path)
        src = self.db_path.expanduser()
        self.tmp_db = Path(tempfile.gettempdir()) / "chat_copy.db"
        try:
            shutil.copy2(src, self.tmp_db)  # requires read permission on Messages.db
        except PermissionError as e:
            raise RuntimeError(
                f"Cannot read {src}. Grant Full Disk Access to the Python/Jupyter process (System Settings → Privacy & Security → Full Disk Access) and re-run."
            ) from e

    def read_messages(self):
        con = sqlite3.connect(self.tmp_db)
        self.messages_df = pd.read_sql("SELECT * FROM message ORDER BY date DESC", con)
        con.close()

    def read_chats(self):
        con = sqlite3.connect(self.tmp_db)
        self.chats_df = pd.read_sql("SELECT * FROM chat ORDER BY last_message_date DESC", con)
        con.close()

    def read_schemas(self):
        con = sqlite3.connect(self.tmp_db)
        cur = con.cursor()
        res = cur.execute("SELECT sql FROM sqlite_master WHERE type='table'")
        self.schemas = res.fetchall()
        res.close()
        cur.close()
        con.close()
