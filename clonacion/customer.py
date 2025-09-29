import mysql.connector
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()



conn_old = mysql.connector.connect(
    host=os.getenv("DB_HOST_pwa1"),
    user= os.getenv("DB_USER_pwa1")  ,
    password= os.getenv("DB_PASSWORD_pwa1")  ,
    database= os.getenv("DB_NAME_pwa1")  ,
    ssl_disabled=True
)


conn_new = mysql.connector.connect(
    host=os.getenv("DB_HOST_pwa2"),
    user= os.getenv("DB_USER_pwa2")  ,
    password= os.getenv("DB_PASSWORD_pwa2")  ,
    database= os.getenv("DB_NAME_pwa2")  ,
    ssl_disabled=True
)


id_job_center_origin = os.getenv("UUID_ORIGINAL_pwa1")
id_job_center = os.getenv("UUID_NUEVO")
bussines_activity = os.getenv("UUID_BUSSINESS")

cursor_old = conn_old.cursor(dictionary=True)
cursor_new = conn_new.cursor()

try:
    cursor_new.execute("""
        SELECT folio FROM customers_customer
        WHERE job_center_id = %s
    """, (id_job_center.replace("-", ""),))
    rows = cursor_new.fetchall()
    print(rows) 
    
    existing_folios = {row[0] for row in rows}
    cursor_old.execute("""
        SELECT c.*, cd.contact_two_name, cd.address, cd.email, cd.phone_number,
               cd.billing, cd.created_at AS cd_created, cd.address_number,
               cd.fiscal_regime, cd.rfc
        FROM customers AS c
        JOIN customer_datas AS cd ON c.id = cd.customer_id
        WHERE c.id_profile_job_center = %s AND c.is_main = 1;
    """, (id_job_center_origin,))
    
    rows = cursor_old.fetchall()

    for row in rows:
        folio = f"CL-{row['id']}"
        if folio in existing_folios:
            continue  

        new_id = str(uuid.uuid4()).replace("-", "")
        now = datetime.now()
        full_address = f"{row['address']} {row['address_number'] or ''}".strip()[:255]

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
            row['phone_number'] or row['cellphone'],
            row['email'],
            row['contact_two_name'],
            row['cellphone_main'],
            row['email'],
            full_address,
            None,
            None,
            row['is_main'],
            bussines_activity.replace("-", ""),
            id_job_center.replace("-", ""),
            None,
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
