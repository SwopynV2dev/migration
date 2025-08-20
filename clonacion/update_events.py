import mysql.connector
import uuid
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv

from service_certificate import ServiceCertificatePDF
from service_order import ServiceOrderPDF

now = datetime.now()

source_conn = mysql.connector.connect(
    host="164.92.98.250",
    user="Workbench",
    password="password20@@",
    database="pwa"
)

source_conn_v2 = mysql.connector.connect(
    host="23.21.191.62",
    user="hmcdcarlos",
    password="Canela243.",                                                                                                                                                          
    database="SwopynProd"
)


source_cursor = source_conn.cursor(dictionary=True)     
source_cursor_v2 = source_conn_v2.cursor(dictionary=True)     
load_dotenv()
job_center_origen = os.getenv("UUID_ORIGINAL_pwa1")
id_job_center = os.getenv("UUID_NUEVO")
search = id_job_center.replace("-", "")



def get_employee_id( uuid_job_center, user_name):
    query = """
    SELECT id FROM SwopynProd.employees_employee where  job_center_id= %s and name = %s  and is_deleted = false
    LIMIT 1;
    """
    source_cursor_v2.execute(query, (uuid_job_center, user_name))
    result = source_cursor_v2.fetchone()
    source_cursor_v2.fetchall()
    return result['id'] if result else None


query = """
        SELECT 
            u.name AS user_name,
            so.id_service_order AS service_order_id
        FROM 
            events e
        JOIN 
            service_orders so ON e.id_service_order = so.id
        JOIN 
            quotations q ON so.id_quotation = q.id
        JOIN 
            employees u ON e.id_employee = u.id
        WHERE 
            e.id_job_center = %s
    """
params = [job_center_origen]

source_cursor.execute(query, params)
events = source_cursor.fetchall()
for event in events :
    query = 'SELECT id FROM SwopynProd.events_event where job_center_id = %s and folio = %s and is_deleted = 0 ;'
    source_cursor_v2.execute(query,(search,event.get("service_order_id"),))
    events_new = source_cursor_v2.fetchall()
    employee_id = get_employee_id( search, event.get("user_name"))
    for event_new in events_new :

        source_cursor_v2.execute("""
            DELETE FROM events_event_employee
            WHERE event_id = %s
        """, (event_new.get('id'),))
        source_conn_v2.commit()
        if employee_id is not None :
            source_cursor_v2.execute("""
                INSERT INTO events_event_employee (event_id, employee_id)
                VALUES (%s, %s)
            """, (event_new.get('id'), employee_id))
            print("Filas insertadas:", source_cursor_v2.rowcount) 
            source_conn_v2.commit()
                