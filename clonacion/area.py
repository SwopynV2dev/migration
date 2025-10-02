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

try:
    with conn_old.cursor(dictionary=True) as source_cursor, conn_new.cursor(dictionary=True) as dest_cursor:
        source_cursor.execute("SELECT id FROM pwa.customers WHERE id_profile_job_center = %s;", (id_company_job_center,))
        old_customers = source_cursor.fetchall()

        for old_customer in old_customers:
            # Buscar cliente en nueva BD
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

            source_cursor.execute(
                "SELECT id, text, parent FROM pwa.monitoring_trees WHERE id_monitoring = %s AND parent = 1 ORDER BY id;",
                (id_company_job_center,)
            )
            zones = source_cursor.fetchall()

            for zone in zones:
                insert_zone = """
                    INSERT INTO pest_control_zone (
                        id, is_active, is_deleted, created_at, updated_at, deleted_at, 
                        name, customer_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                zone_id = str(uuid.uuid4()).replace("-", "")
                values_zone = (
                    zone_id, 1, 0, datetime.now(), datetime.now(), None,
                    zone['text'], new_customer['id']
                )
                dest_cursor.execute(insert_zone, values_zone)
                conn_new.commit()
                source_cursor.execute(
                    "SELECT id, name FROM pwa.monitoring_trees WHERE id_monitoring = %s AND id_node = %s ORDER BY id;",
                    (id_company_job_center, zone['id'])
                )
                areas = source_cursor.fetchall()

                for area in areas:
                    insert_nesting = """
                        INSERT INTO pest_nestingarea (
                            id, is_active, is_deleted, created_at, updated_at, deleted_at, 
                            name, check_point, certificate, color, area_category_id, 
                            customer_id, zone_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    nesting_id = str(uuid.uuid4()).replace("-", "")
                    values_nesting = (
                        nesting_id, 1, 0, datetime.now(), datetime.now(), None,
                        area.get("name"),
                        1,         
                        0,        
                        "#FFFFFF",
                        os.getenv("UUID_AREA_CATEGORY"),
                        new_customer['id'],
                        zone_id
                    )
                    dest_cursor.execute(insert_nesting, values_nesting)
                    conn_new.commit()
                    source_cursor.execute(
                        "SELECT id, `key`, name, station_type_id FROM pwa.monitoring_trees WHERE id_monitoring = %s AND id_node = %s ORDER BY id;",
                        (id_company_job_center, area['id'])
                    )
                    stations = source_cursor.fetchall()

                    for station in stations:
                        insert_station = """
                            INSERT INTO pest_control_areastation (
                                id, is_active, is_deleted, created_at, updated_at, deleted_at,
                                `key`, name, station_type_id, nesting_area_id, old_id
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
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
                            station.get("station_type_id"),
                            nesting_id,
                            station.get("id"),  
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
