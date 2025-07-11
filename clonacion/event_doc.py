import mysql.connector
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


def get_service_type_id( uuid_job_center, service_name):
    query = """
    SELECT id FROM catalogs_servicetype
    WHERE job_center_id = %s AND name = %s
    LIMIT 1;
    """
    source_cursor_v2.execute(query, (uuid_job_center, service_name))
    result = source_cursor_v2.fetchone()
    source_cursor_v2.fetchall()
    return result['id'] if result else None



def parse_time_safe(val):
    if val and isinstance(val, str):
        try:
            if len(val.split(":")) == 2:
                return datetime.strptime(val, "%H:%M").time()
            elif len(val.split(":")) == 3:
                return datetime.strptime(val, "%H:%M:%S").time()
        except ValueError:
            print("Error parsing time:", val)
    return None


def get_plague_ids(uuid_job_center, plagues):
    if not plagues:
        return []
    plagues_list = [p.strip() for p in plagues.split(',') if p.strip()]
    if not plagues_list:
        return []
    placeholders = ', '.join(['%s'] * len(plagues_list))
    query = f"""
        SELECT id FROM catalogs_plague
        WHERE name IN ({placeholders})
        AND job_center_id = %s
    """
    source_cursor_v2.execute(query, tuple(plagues_list + [uuid_job_center]))
    return [row['id'] for row in source_cursor_v2.fetchall()]

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

query = 'SELECT folio FROM SwopynProd.events_event where job_center_id = %s;'
source_cursor_v2.execute(query, (search,))
excluded_folios = [row['folio'] for row in source_cursor_v2.fetchall()]  

for quote in quotes:
    source_cursor_v2.execute("""
        SELECT id FROM SwopynProd.customers_customer WHERE folio = %s;
    """, (f'CL-{quote.get("custom")}',))
    res = source_cursor_v2.fetchone()
    source_cursor_v2.fetchall()

    if res:
        custom = res['id']
        source_cursor_v2.execute("""
            SELECT id FROM SwopynProd.quotes_quote WHERE folio = %s;
        """, (quote.get('id_quotation'),))
        quote_row = source_cursor_v2.fetchone()
        source_cursor_v2.fetchall()
        if not quote_row:
            continue
        quote_id = quote_row['id']

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
                    users u ON e.emloyee_id = u.id
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

        for event in events:
            source_cursor.execute("""
                SELECT json
                FROM pwa.app_services_log
                WHERE event_id = %s;
            """, (event.get('id'),))
            rows = source_cursor.fetchall()

            raw_start = None
            raw_end = None
            for r in rows:
                try:
                    import json
                    log_data = json.loads(r['json'])
                    raw_start = log_data.get('start_event')
                    raw_end = log_data.get('finish_event')
                    break
                except json.JSONDecodeError as e:
                    print(f"⚠️ JSON inválido para event_id={event.get('id')}: {e}")
                    continue

            if raw_start or raw_end:
                status = 'd266c0ae53dc478391cdc62044601369'
                start = parse_time_safe(raw_start)
                end = parse_time_safe(raw_end)

                if start and not end:
                    dt_start = datetime.combine(datetime.today(), start)
                    dt_end = dt_start + timedelta(hours=1)
                    end = dt_end.time()
                elif end and not start:
                    dt_end = datetime.combine(datetime.today(), end)
                    dt_start = dt_end - timedelta(hours=1)
                    start = dt_start.time()
            else:
                status = 'fa0026238b18446d812996797f5a2b6e'
                start = end = None

            if event.get('id_status') == 2:
                status = 'd198f9d9854a4d388c9dbb0413c46d77'

            # Buscar evento ya insertado
            source_cursor_v2.execute("""
                SELECT id FROM events_event
                WHERE folio = %s AND job_center_id = %s
            """, (event.get('id_service_order'), search))
            existing_event = source_cursor_v2.fetchone()

            if existing_event:
                existing_event_id = existing_event[0]

                employee_id = get_employee_id(search, event.get("user_name"))
                service_type_id = get_service_type_id(search, quote.get("price_list_name"))
                plague_ids = get_plague_ids(search, quote.get("plagues"))

                # Actualiza campos
                source_cursor_v2.execute("""
                    UPDATE events_event
                    SET updated_at = %s,
                        real_initial_hour = %s,
                        real_final_hour = %s,
                        status_id = %s
                    WHERE id = %s
                """, (
                    datetime.now(),
                    start,
                    end,
                    status,
                    existing_event_id
                ))
                if employee_id:
                    source_cursor_v2.execute("INSERT INTO events_event_employee (event_id, employee_id) VALUES (%s, %s)", (existing_event_id, employee_id))


                source_cursor_v2.execute("INSERT INTO events_event_plague (event_id, plague_id) VALUES (%s, %s)", (existing_event_id, os.getenv("PLAGUE").replace("-", "")))

                source_cursor_v2.execute("INSERT INTO events_event_group (event_id, group_id) VALUES (%s, %s)", (existing_event_id, os.getenv("GROUP").replace("-", "")))

                source_conn_v2.commit()

                # Generar PDFs si está completado
                #if status == 'd266c0ae53dc478391cdc62044601369':
                 #   ServiceCertificatePDF.build(event.get('service_order_id'), existing_event_id)
                 #   ServiceOrderPDF.build(event.get('service_order_id'), existing_event_id)
            else:
                print(f"⚠️ Evento no encontrado para folio {event.get('id_service_order')} — no se actualizó.")
