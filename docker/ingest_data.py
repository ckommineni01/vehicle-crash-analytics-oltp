import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# Config (Docker-first)

DB_URL = os.getenv("DB_URL")

if not DB_URL:
    
    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "127.0.0.1"
    DB_PORT = "5433"
    DB_NAME = "collisions"
    DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

CSV_FILE = os.getenv("CSV_FILE", "motor_vehicle_collisions_2023_2024.csv")


engine = create_engine(DB_URL, pool_pre_ping=True)

# Helpers

def wait_for_db(max_retries=30, sleep_sec=2):
    """Wait until DB is ready (important for docker-compose)."""
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ DB is ready")
            return True
        except Exception as e:
            print(f"⏳ Waiting for DB... ({i+1}/{max_retries}) {e}")
            time.sleep(sleep_sec)
    return False


def clean_df(df):
    df = df.where(pd.notnull(df), None)

    
    df["collision_id"] = pd.to_numeric(df["collision_id"], errors="coerce")

    
    df = df[df["collision_id"].notnull()].copy()
    df["collision_id"] = df["collision_id"].astype("int64")

    
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.date
    df["crash_time"] = pd.to_datetime(df["crash_time"], errors="coerce").dt.time

    return df


def get_or_create(conn, table, id_col, value_col, value):
    """
    Generic get-or-create for dimension tables.
    table: e.g. "vehicle_types"
    id_col: e.g. "vehicle_type_id"
    value_col: e.g. "vehicle_type_desc"
    """
    sel = text(f"SELECT {id_col} FROM {table} WHERE {value_col} = :val")
    res = conn.execute(sel, {"val": value}).fetchone()
    if res:
        return res[0]

    ins = text(f"""
        INSERT INTO {table} ({value_col})
        VALUES (:val)
        ON CONFLICT DO NOTHING
        RETURNING {id_col};
    """)
    res = conn.execute(ins, {"val": value}).fetchone()

  
    if not res:
        res = conn.execute(sel, {"val": value}).fetchone()
        return res[0] if res else None

    return res[0]


# Load CSV

def load_csv():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(
            f"CSV file not found: {CSV_FILE}\n"
            f"Put it in the repo root or set CSV_FILE env var."
        )
    df = pd.read_csv(CSV_FILE)
    df = clean_df(df)
    return df



# ETL Steps

def load_boroughs(df):
    unique_boros = df["borough"].dropna().unique()

    with engine.begin() as conn:
        for b in unique_boros:
            b = str(b).strip()
            if not b:
                continue
            conn.execute(
                text("""
                    INSERT INTO boroughs (borough_name)
                    VALUES (:b)
                    ON CONFLICT DO NOTHING;
                """),
                {"b": b}
            )
    print("boroughs loaded")


def load_collisions(df):
    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        for r in rows:
            borough_id = None
            if r.get("borough"):
                q = conn.execute(
                    text("SELECT borough_id FROM boroughs WHERE borough_name = :b"),
                    {"b": r["borough"]}
                ).fetchone()
                borough_id = q[0] if q else None

            conn.execute(
                text("""
                    INSERT INTO collisions (
                        collision_id, crash_date, crash_time,
                        borough_id, zip_code, latitude, longitude,
                        location, on_street_name, off_street_name, cross_street_name,
                        number_of_persons_injured, number_of_persons_killed,
                        number_of_pedestrians_injured, number_of_pedestrians_killed,
                        number_of_cyclist_injured, number_of_cyclist_killed,
                        number_of_motorist_injured, number_of_motorist_killed
                    )
                    VALUES (
                        :collision_id, :crash_date, :crash_time,
                        :borough_id, :zip_code, :latitude, :longitude,
                        :location, :on_street_name, :off_street_name, :cross_street_name,
                        :num_inj, :num_killed,
                        :ped_inj, :ped_killed,
                        :cyc_inj, :cyc_killed,
                        :mot_inj, :mot_killed
                    )
                    ON CONFLICT DO NOTHING;
                """),
                {
                    "collision_id": int(r["collision_id"]),
                    "crash_date": r.get("crash_date"),
                    "crash_time": r.get("crash_time"),
                    "borough_id": borough_id,
                    "zip_code": r.get("zip_code"),
                    "latitude": r.get("latitude"),
                    "longitude": r.get("longitude"),
                    "location": r.get("location"),
                    "on_street_name": r.get("on_street_name"),
                    "off_street_name": r.get("off_street_name"),
                    "cross_street_name": r.get("cross_street_name"),
                    "num_inj": r.get("number_of_persons_injured"),
                    "num_killed": r.get("number_of_persons_killed"),
                    "ped_inj": r.get("number_of_pedestrians_injured"),
                    "ped_killed": r.get("number_of_pedestrians_killed"),
                    "cyc_inj": r.get("number_of_cyclist_injured"),
                    "cyc_killed": r.get("number_of_cyclist_killed"),
                    "mot_inj": r.get("number_of_motorist_injured"),
                    "mot_killed": r.get("number_of_motorist_killed")
                }
            )

    print("collisions loaded")


def load_vehicles(df):
    with engine.begin() as conn:
        for _, r in df.iterrows():
            cid = int(r["collision_id"])

            for i in range(1, 6):
                
                if i <= 2:
                    col = f"vehicle_type_code{i}"
                else:
                    col = f"vehicle_type_code_{i}"

                vt = r.get(col)
                if vt and isinstance(vt, str) and vt.strip():
                    vt = vt.strip()
                    vt_id = get_or_create(
                        conn,
                        table="vehicle_types",
                        id_col="vehicle_type_id",
                        value_col="vehicle_type_desc",
                        value=vt
                    )
                    if vt_id is None:
                        continue
                    conn.execute(
                        text("""
                            INSERT INTO collision_vehicles (collision_id, vehicle_order, vehicle_type_id)
                            VALUES (:cid, :order, :vt)
                            ON CONFLICT DO NOTHING;
                        """),
                        {"cid": cid, "order": i, "vt": vt_id}
                    )

    print("vehicles loaded")


def load_factors(df):
    with engine.begin() as conn:
        for _, r in df.iterrows():
            cid = int(r["collision_id"])

            for i in range(1, 6):
                col = f"contributing_factor_vehicle_{i}"
                fac = r.get(col)

                if fac and isinstance(fac, str):
                    fac = fac.strip()
                    if fac and fac.upper() != "UNSPECIFIED":
                        fac_id = get_or_create(
                            conn,
                            table="factors",
                            id_col="factor_id",
                            value_col="factor_desc",
                            value=fac
                        )
                        if fac_id is None:
                            continue
                        conn.execute(
                            text("""
                                INSERT INTO collision_factors (collision_id, factor_order, factor_id)
                                VALUES (:cid, :order, :fid)
                                ON CONFLICT DO NOTHING;
                            """),
                            {"cid": cid, "order": i, "fid": fac_id}
                        )

    print("factors loaded")



# Main

if __name__ == "__main__":
    print("\nStarting ETL ingestion...\n")
    print("DB_URL =", DB_URL)
    print("CSV_FILE =", CSV_FILE)

    if not wait_for_db():
        print("❌ DB never became ready. Exiting.")
        raise SystemExit(1)

    df = load_csv()

    try:
        load_boroughs(df)
        load_collisions(df)
        load_vehicles(df)
        load_factors(df)
        print("\n✅ data loaded successfully\n")
    except SQLAlchemyError as e:
        print("\n❌ ETL failed:\n", e)
        raise
