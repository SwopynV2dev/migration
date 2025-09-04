import mysql.connector
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

cursor_old = conn_old.cursor(dictionary=True)
cursor_new = conn_new.cursor(dictionary=True)

try:
    cursor_old.execute("""
        SELECT c.id, 
               cd.address, cd.address_number, cd.state, cd.reference_address
        FROM customers AS c
        JOIN customer_datas AS cd ON c.id = cd.customer_id
        WHERE c.id_profile_job_center = %s;
    """, (id_job_center_origin,))
    
    rows = cursor_old.fetchall()

    for row in rows:
        folio = f'CL-{row["id"]}'

    
        parts = [
            row['address'] or "",
            row['address_number'] or "",
            row['state'] or "",
            row['reference_address'] or ""
        ]

        full_address = ", ".join([p.strip() for p in parts if p and p.strip()])[:255]
        print(full_address)
        update_query = """
            UPDATE customers_customer
            SET address = %s
            WHERE folio = %s AND job_center_id = %s;
        """
        cursor_new.execute(update_query, (full_address, folio, id_job_center))

    conn_new.commit()
    print("✅ Direcciones actualizadas correctamente")

finally:
    cursor_old.close()
    cursor_new.close()
    conn_old.close()
    conn_new.close()
