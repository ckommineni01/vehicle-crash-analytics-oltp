import pandas as pd
from sqlalchemy import create_engine, text

DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "127.0.0.1"

DB_PORT = "5433"
DB_NAME = "collisions"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

csv_file = "motor_vehicle_collisions_2023_2024.csv"
df = pd.read_csv(csv_file)

df = df.where(pd.notnull(df), None)

df["collision_id"] = df["collision_id"].astype("int64")
df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce").dt.date
df["crash_time"] = pd.to_datetime(df["crash_time"], errors="coerce").dt.time


def get_or_create(conn, table, column, value):
    select_q = f"SELECT {table[:-1]}_id FROM {table} WHERE {column} = :val"
    res = conn.execute(text(select_q), {"val": value}).fetchone()
    if res:
        return res[0]

    insert_q = f"""
        INSERT INTO {table} ({column})
        VALUES (:val)
        RETURNING {table[:-1]}_id;
    """
    res = conn.execute(text(insert_q), {"val": value}).fetchone()
    return res[0]


def load_boroughs():
    unique_boros = df["borough"].dropna().unique()

    with engine.begin() as conn:
        for b in unique_boros:
            conn.execute(
                text("""
                    INSERT INTO boroughs (borough_name)
                    VALUES (:b)
                    ON CONFLICT DO NOTHING;
                """),
                {"b": b}
            )
    print("boroughs loaded")


def load_collisions():
    rows = df.to_dict(orient="records")

    with engine.begin() as conn:
        for r in rows:
            borough_id = None
            if r["borough"]:
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
                    "collision_id": r["collision_id"],
                    "crash_date": r["crash_date"],
                    "crash_time": r["crash_time"],
                    "borough_id": borough_id,
                    "zip_code": r["zip_code"],
                    "latitude": r["latitude"],
                    "longitude": r["longitude"],
                    "location": r["location"],
                    "on_street_name": r["on_street_name"],
                    "off_street_name": r["off_street_name"],
                    "cross_street_name": r["cross_street_name"],
                    "num_inj": r["number_of_persons_injured"],
                    "num_killed": r["number_of_persons_killed"],
                    "ped_inj": r["number_of_pedestrians_injured"],
                    "ped_killed": r["number_of_pedestrians_killed"],
                    "cyc_inj": r["number_of_cyclist_injured"],
                    "cyc_killed": r["number_of_cyclist_killed"],
                    "mot_inj": r["number_of_motorist_injured"],
                    "mot_killed": r["number_of_motorist_killed"]
                }
            )

    print("collisions loaded")


def load_vehicles():
    with engine.begin() as conn:
        for _, r in df.iterrows():
            cid = int(r["collision_id"])

            for i in range(1, 6):
                vt = r.get(f"vehicle_type_code{i}")
                if vt and isinstance(vt, str) and vt.strip():
                    vt_id = get_or_create(conn, "vehicle_types", "vehicle_type_desc", vt)
                    conn.execute(
                        text("""
                            INSERT INTO collision_vehicles (collision_id, vehicle_order, vehicle_type_id)
                            VALUES (:cid, :order, :vt)
                            ON CONFLICT DO NOTHING;
                        """),
                        {"cid": cid, "order": i, "vt": vt_id}
                    )

    print("vehicles loaded")


def load_factors():
    with engine.begin() as conn:
        for _, r in df.iterrows():
            cid = int(r["collision_id"])

            for i in range(1, 6):
                fac = r.get(f"contributing_factor_vehicle_{i}")

                if fac and isinstance(fac, str):
                    fac = fac.strip()
                    if fac and fac.upper() != "UNSPECIFIED":
                        fac_id = get_or_create(conn, "factors", "factor_desc", fac)

                        conn.execute(
                            text("""
                                INSERT INTO collision_factors (collision_id, factor_order, factor_id)
                                VALUES (:cid, :order, :fid)
                                ON CONFLICT DO NOTHING;
                            """),
                            {"cid": cid, "order": i, "fid": fac_id}
                        )

    print("factors loaded")



if __name__ == "__main__":
    print("\nStarting ETL ingestion...\n")
    load_boroughs()
    load_collisions()
    load_vehicles()
    load_factors()
    print("\n data loaded successfully\n")
