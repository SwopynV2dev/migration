import mysql.connector
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

conn_old = mysql.connector.connect(
    host="164.92.98.250",
    user="Workbench",
    password="password20@@",
    database="pwa"
)
conn_new = mysql.connector.connect(
    host="23.21.191.62",
    user="hmcdcarlos",
    password="Canela243.",
    database="SwopynProd"
)

id_job_center_origin = os.getenv("UUID_ORIGINAL_pwa1")
id_job_center = os.getenv("UUID_NUEVO").replace("-", "")
bussines_activity = os.getenv("UUID_BUSSINESS").replace("-", "")

cursor_old = conn_old.cursor(dictionary=True)
cursor_new = conn_new.cursor(dictionary=True)
try:
    # Obtener los folios ya existentes en el nuevo job_center
    cursor_new.execute("""
        SELECT folio FROM customers_customer
        WHERE job_center_id = %s
    """, (id_job_center,))
    rows = cursor_new.fetchall()
    print(rows) 
    
    existing_folios = {row['folio'] for row in rows}
    # Traer clientes secundarios del origen
    cursor_old.execute("""
        SELECT c.*, cd.contact_two_name, cd.address, cd.email, cd.phone_number,
            cd.billing, cd.created_at AS cd_created, cd.address_number,
            cd.fiscal_regime, cd.rfc, c.customer_main_id
        FROM customers AS c
        JOIN customer_datas AS cd ON c.id = cd.customer_id
        WHERE c.id_profile_job_center = %s AND c.is_main = 0;
    """, (id_job_center_origin,))
    
    rows = cursor_old.fetchall()

    for row in rows:
        folio = f'CL-{row["id"]}'
        if folio in existing_folios:
            continue  # Ya existe, lo saltamos

        new_id = str(uuid.uuid4()).replace("-", "")
        now = datetime.now()
        full_address = f"{row['address']} {row['address_number'] or ''}".strip()[:255]

        # Obtener el ID del cliente principal ya insertado
        cursor_new.execute("""
            SELECT id FROM customers_customer
            WHERE folio = %s AND job_center_id = %s;
        """, (f'CL-{row["customer_main_id"]}', id_job_center))
        result = cursor_new.fetchone()
        cursor_new.fetchall()  # Limpiar el cursor por si acaso
        main_customer_new_id = result['id'] if result else None

        insert_query = """
            INSERT INTO customers_customer (
                id, is_active, is_deleted, created_at, updated_at, deleted_at,
                name, folio, phone, email, contact_name, contact_phone, contact_email,
                address, address_latitude, address_longitude, is_main,
                business_activity_id, job_center_id, main_customer_id,
                internal_id, logo, reason_social, taxpayer_registration
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        insert_values = (
            new_id,
            1,
            0,
            row['cd_created'] if row['cd_created'] else now,
            now,
            now,
            row['name'],
            folio,
            row['phone_number'] or row.get('cellphone'),
            row['email'],
            row['contact_two_name'],
            row.get('cellphone_main'),
            row['email'],
            full_address,
            None,
            None,
            row['is_main'],
            bussines_activity,
            id_job_center,
            main_customer_new_id,
            f"INT-{row['id']}",
            None,
            row['fiscal_regime'],
            row['rfc']
        )

        cursor_new.execute(insert_query, insert_values)
        conn_new.commit()

finally:
    cursor_old.close()
    cursor_new.close()
    conn_old.close()
    conn_new.close()

