import requests
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime
import uuid

load_dotenv()

conn_old = mysql.connector.connect(
    host=os.getenv("DB_HOST_pwa1"),
    user=os.getenv("DB_USER_pwa1"),
    password=os.getenv("DB_PASSWORD_pwa1"),
    database=os.getenv("DB_NAME_pwa1"),
    ssl_disabled=True
)

conn_new = mysql.connector.connect(
    host=os.getenv("DB_HOST_pwa2"),
    user=os.getenv("DB_USER_pwa2"),
    password=os.getenv("DB_PASSWORD_pwa2"),
    database=os.getenv("DB_NAME_pwa2"),
    ssl_disabled=True
)

uuid_nuevo = os.getenv("UUID_NUEVO")               
id_company_job_center = os.getenv("UUID_ORIGINAL_pwa1")  
search = uuid_nuevo.replace("-", "")

now = datetime.now()

try:
    with conn_old.cursor(dictionary=True) as source_cursor, conn_new.cursor(dictionary=True) as dest_cursor:

        source_cursor.execute("SELECT id FROM pwa.customers WHERE id_profile_job_center = %s;", (id_company_job_center,))
        old_customers = source_cursor.fetchall()

        for old_customer in old_customers:
            customer_uuid = str(uuid.uuid4())

            check_query = """
                SELECT * 
                FROM SwopynProd.customers_customer 
                WHERE folio = %s AND job_center_id = %s;
            """
            dest_cursor.execute(check_query, (f"CL-{old_customer['id']}", search))
            new_customer = dest_cursor.fetchone()
            dest_cursor.fetchall()

            if not new_customer:
                print(f"⚠️ Cliente {old_customer['id']} no encontrado en nueva BD, se omite.")
                continue

            insert_zone = """
                INSERT INTO pest_control_zone (
                    id, is_active, is_deleted, created_at, updated_at, deleted_at, 
                    name, customer_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            zone_id = str(uuid.uuid4()).replace("-", "")
            values_zone = (
                zone_id, 1, 0, datetime.now(), datetime.now(), None,
                "General", new_customer['id']
            )
            dest_cursor.execute(insert_zone, values_zone)
            conn_new.commit()

            source_cursor.execute("SELECT * FROM pwa.areas WHERE customer_id = %s;", (old_customer["id"],))
            areas = source_cursor.fetchall()

            for area in areas:
                insert_nesting = """
                    INSERT INTO business_pest_nestingarea (
                        id, is_active, is_deleted, created_at, updated_at, deleted_at, 
                        name, check_point, certificate, color, area_category_id, 
                        business_activity_id, zone_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                nesting_id = str(uuid.uuid4()).replace("-", "")
                values_nesting = (
                    nesting_id, 1, 0, datetime.now(), datetime.now(), None,
                    area.get("name"),
                    area.get("check_point") if area.get("check_point") is not None else 0,
                    area.get("certificate") if area.get("certificate") is not None else 0,
                    area.get("color"),
                    area.get("area_category_id"),   # 👈 ojo, lo tomé del área original
                    area.get("business_activity_id"),
                    zone_id
                )
                dest_cursor.execute(insert_nesting, values_nesting)
                conn_new.commit()

                source_cursor.execute("SELECT * FROM pwa.monitoring_trees WHERE customer_id = %s;", (old_customer["id"],))
                stations = source_cursor.fetchall()

                for station in stations:
                    insert_station = """
                        INSERT INTO pest_control_areastation (
                            id, is_active, is_deleted, created_at, updated_at, deleted_at,
                            key, name, nesting_area_id, station_type_id, qr,
                            x_coordinate, y_coordinate, product_id, old_id
                        ) VALUES (%s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s,
                                %s, %s, %s, %s)
                    """
                    station_id = str(uuid.uuid4()).replace("-", "")

                    values_station = (
                        station_id,
                        1,
                        0,
                        datetime.now(),
                        datetime.now(),
                        None,
                        station.get("key"),
                        station.get("name"),
                        nesting_id,  
                        station.get("station_type_id"),
                        station.get("qr"),
                        station.get("x_coordinate"),
                        station.get("y_coordinate"),
                        station.get("product_id"),
                        station.get("old_id"),
                    )
                    dest_cursor.execute(insert_station, values_station)
                    conn_new.commit()

except mysql.connector.Error as e:
    print(f"❌ Error con la base de datos: {e}")
except requests.exceptions.RequestException as re:
    print(f"❌ Error en la solicitud a la API: {re}")
except Exception as ex:
    print(f"❌ Error inesperado: {ex}")
finally:
    conn_old.close()
    conn_new.close()
