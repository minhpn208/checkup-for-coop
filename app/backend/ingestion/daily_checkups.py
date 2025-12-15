from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from app.backend.helpers.postgres_helper import insert_dataframe

def ingest_daily_checkups() -> Dict[str, Any]:
    checkup = input("Enter a daily checkup: ")
    if checkup == "":
        print("No checkup provided. Exiting...")
        return None
    checkup_map = {
        "entry_time": datetime.now().strftime("%Y-%m-%d"),
        "checkup": checkup
    }
    return checkup_map

def export_to_db(total_checkups: Dict[str, Any]) -> None:
    df = pd.DataFrame([total_checkups])
    if df.empty:
        print("No checkup provided. Exiting...")
        return None
    insert_dataframe(df, "public", "daily_checkups")
    print("Checkup exported to database successfully.")

if __name__ == "__main__":
    total_checkups = ingest_daily_checkups()
    export_to_db(total_checkups)