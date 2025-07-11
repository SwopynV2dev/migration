import mysql.connector
import uuid
from datetime import datetime, timedelta
import os
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
    SELECT id FROM SwopynProd.employees_employee where  job_center_id= %s and name = %s 
    LIMIT 1;
    """
    source_cursor_v2.execute(query, (uuid_job_center, user_name))
    result = source_cursor_v2.fetchone()
    source_cursor_v2.fetchall()
    return result['id'] if result else None




def convert_datetime(value):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%dT%H:%M:%S')
    return value

source_cursor.execute("""
    SELECT q.id,q.id_customer as custom, pr.name as price_list_name ,
    q.id_quotation FROM pwa.quotations as q
    LEFT JOIN price_lists pr ON q.id_price_list = pr.id
    WHERE id_job_center = %s ;
""", (job_center_origen,))
quotes = source_cursor.fetchall()
source_cursor.fetchall()

query = 'SELECT folio FROM SwopynProd.events_event where job_center_id = %s;'
source_cursor_v2.execute(query,(search,))
excluded_folios = [row['folio'] for row in source_cursor_v2.fetchall()]


for quote in quotes : 
    source_cursor_v2.execute("""
    SELECT id FROM SwopynProd.customers_customer where folio = %s;
    """,(f'CL-{quote.get('custom')}',))
    res = source_cursor_v2.fetchone()
    source_cursor_v2.fetchall()
    if res : 
        custom = res.get('id')
        source_cursor_v2.execute("""
        SELECT id FROM SwopynProd.quotes_quote where folio = %s;
        """,(f'{quote.get('id_quotation')}',))
        quote_id = source_cursor_v2.fetchone().get('id') 
        source_cursor_v2.fetchall()
        if excluded_folios:
            placeholders = ', '.join(['%s'] * len(excluded_folios))
            query = f"""
                SELECT 
                    e.id,
                    e.title AS event_title,
                    e.initial_date,
                    e.final_date,
                    e.created_at,
                    e.initial_hour,
                    e.final_hour,
                    e.service_type,
                    u.name AS user_name,
                    so.id AS service_order_id,
                    so.total AS service_order_total,
                    so.id_service_order,
                    so.id_status,
                    st.name AS service_type_name
                FROM 
                    events e
                JOIN 
                    service_orders so ON e.id_service_order = so.id
                JOIN 
                    quotations q ON so.id_quotation = q.id
                JOIN 
                    users u ON so.user_id = u.id
                JOIN 
                    type_services st ON e.service_type = st.id
                WHERE 
                    q.id = %s AND so.id_service_order NOT IN ({placeholders})
            """
            params = [quote.get('id')] + excluded_folios
        else:
            query = """
                SELECT 
                    e.id,
                    e.title AS event_title,
                    e.initial_date,
                    e.final_date,
                    e.created_at,
                    e.initial_hour,
                    e.final_hour,
                    e.service_type,
                    u.name AS user_name,
                    so.id AS service_order_id,
                    so.total AS service_order_total,
                    so.id_service_order,
                    so.id_status,
                    st.name AS service_type_name
                FROM 
                    events e
                JOIN 
                    service_orders so ON e.id_service_order = so.id
                JOIN 
                    quotations q ON so.id_quotation = q.id
                JOIN 
                    users u ON so.user_id = u.id
                JOIN 
                    type_services st ON e.service_type = st.id
                WHERE 
                    q.id = %s
            """
            params = [quote.get('id')]

        source_cursor.execute(query, params)
        events = source_cursor.fetchall()

        for event in events :
            
            employee_id = get_employee_id( search, event.get("user_name"))
            
            source_cursor_v2.execute("""
            INSERT INTO events_event (
                id,
                is_active,
                is_deleted,
                created_at,
                updated_at,
                deleted_at,
                title,
                folio,
                initial_date,
                final_date,
                initial_hour,
                final_hour,
                real_initial_date,
                real_final_date,
                real_initial_hour,
                real_final_hour,
                start_latitude,
                start_longitude,
                end_latitude,
                end_longitude,
                comments,
                customer_id,
                event_type_id,
                job_center_id,
                quote_id,
                service_type_id,
                status_id,
                ticket_id,
                rrule,
                custom_dates,
                total,
                is_synchronized,
                certificate_email,
                is_infinty,
                is_many,
                indications,
                main_event_id,
                certificate_whats,
                reminder,
                created_by_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                event_id.hex,
                1,              
                0,              
                event.get("created_at") or now,
                now,           
                now,           
                event.get("event_title"),
                event.get('id_service_order'),           
                event.get("initial_date"),
                event.get("final_date"),
                event.get("initial_hour"),
                event.get("final_hour"),
                event.get("final_date"),           # real_initial_date
                event.get("final_date"),           # real_final_date
                start,           # real_initial_hour
                end,           # real_final_hour
                None,           # start_latitude
                None,           # start_longitude
                None,           # end_latitude
                None,           # end_longitude
                None,           # comments
                custom,           # customer_id
                "13116023e65a4a3a9e1c82802b7b2ea0",
                search,
                quote_id or None,
                service_type_id,
                status,           # status_id
                None,           # ticket_id
                None,           # rrule
                None,           # custom_dates
                event.get("service_order_total") or 0,
                0,              # is_synchronized
                0,              # certificate_email
                0,              # is_infinty
                0,              # is_many
                0,              # indications   
                None,           # main_event_id
                0,             
                0,             
                None   
            ))  
            source_conn_v2.commit()
            #if status == 'd266c0ae53dc478391cdc62044601369':
            #    ServiceCertificatePDF.build(event.get('service_order_id'),event_id)
            #    ServiceOrderPDF.build(event.get('service_order_id'),event_id)
    