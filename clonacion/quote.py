import mysql.connector
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv


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
uuid_nuevo = os.getenv("UUID_NUEVO")


search = uuid_nuevo.replace("-", "")



def get_employee_id( uuid_job_center, user_name):
    query = """
    SELECT id FROM SwopynProd.employees_employee where  job_center_id= %s and name = %s 
    LIMIT 1;
    """
    source_cursor_v2.execute(query, (uuid_job_center, user_name))
    result = source_cursor_v2.fetchone()
    return result['id'] if result else None


def get_service_type_id( uuid_job_center, service_name):
    query = """
    SELECT id ,description FROM catalogs_servicetype
    WHERE job_center_id = %s AND name = %s
    LIMIT 1;
    """
    source_cursor_v2.execute(query, (uuid_job_center, service_name))
    result = source_cursor_v2.fetchone()
    return result


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





source_cursor.execute("""
    SELECT id from customers
    WHERE id_profile_job_center = %s ;
""", (job_center_origen,))

customers_old = source_cursor.fetchall()
source_cursor_v2.execute("""
    SELECT folio FROM quotes_quote
    WHERE job_center_id = %s;
""", (search,))
row_exist = source_cursor_v2.fetchall()
existing_folios = {row['folio'] for row in row_exist}

for customer_old in customers_old:
    
    source_cursor_v2.execute("""
                SELECT id from customers_customer
                WHERE folio = %s and job_center_id = %s;
                             """,(f'CL-{customer_old.get('id')}',search,))
    customer_new = source_cursor_v2.fetchone()
    source_cursor_v2.fetchall()
    source_cursor.execute("""
    SELECT 
    q.id AS quotation_id, 
    q.id_quotation AS folio, 
    q.id_customer, 
    q.description, 
    q.total, 
    q.price, 
    q.created_at,
    u.name AS user_name, 
    u.email AS user_email, 
    s.name AS status_name, 
    pr.name AS price_list_name,
    GROUP_CONCAT(pt.name SEPARATOR ', ') AS plagues
FROM quotations q
JOIN users u ON q.user_id = u.id
LEFT JOIN price_lists pr ON q.id_price_list = pr.id
LEFT JOIN statuses s ON q.id_status = s.id
LEFT JOIN plague_type_quotation ptq ON q.id = ptq.quotation_id
LEFT JOIN plague_types pt ON ptq.plague_type_id = pt.id
WHERE q.id_customer = %s
GROUP BY 
    q.id, q.id_quotation, q.id_customer, q.description, q.total, q.price, q.created_at,
    u.name, u.email, s.name, pr.name;
    """, (customer_old.get('id'),))
    quotations = source_cursor.fetchall()

    for quotation in quotations:
        if quotation.get("folio") in existing_folios:
            continue    
        employee_id = get_employee_id( search, quotation.get("user_name"))
        service_type_id = get_service_type_id( search, quotation.get("price_list_name"))
        plague_ids = get_plague_ids( search, quotation.get("plagues"))       
        status = 'd8da652f743042c3bdf89365b9ebc386'
        if quotation.get('status_name') =='Programado':
            status ='683ab9a3ae19453aa0224871b94bd5c0'
        if quotation.get('status_name') =='Cancelado':
            status ='e4e892ddd4414019835aadab616ecce8'
        if quotation.get('status_name') =='Rechazado':
            status ='90ad9fbae4d1415e80db04fc10f3c9b8'
        
        query = """
        INSERT INTO quotes_quote (
            id, is_active, is_deleted, created_at, updated_at,deleted_at,
            folio, pdf, customer_id, employee_id, job_center_id,
            service_type_id, status_id, description,
            email, whatsapp, hash
        )
        VALUES (%s, %s, %s, %s, %s,%s,
                %s, %s, %s, %s, 
                %s, %s, %s, %s,
                %s, %s, %s);
        """           
        quote_id = uuid.uuid4().hex
        folio = 1000
        params_quote = (
            quote_id,
            1, 
            0, 
            quotation.get("created_at") if  quotation.get("created_at")  else now,
            now,
            now,
            quotation.get("folio") or f'{(folio+1)}' ,
            '', 
            customer_new.get('id'),
            employee_id,
            search,
            service_type_id.get('id') if service_type_id else None,
            status,  
            quotation.get("description") or (service_type_id.get('description') if service_type_id else None),
            0, 
            0,  
            '',  
        )   
        source_cursor_v2.execute(query, params_quote)
        source_conn_v2.commit()
        for plague in plague_ids : 
            source_cursor_v2.execute("""INSERT INTO quotes_quote_plague ( quote_id, plague_id)
            VALUES (%s,%s);
            """,( quote_id,plague))
            source_conn_v2.commit()
        source_cursor.execute("""
        SELECT * FROM custom_quotes WHERE id_quotation = %s;
        """, (quotation.get('id'),))
        concepts = source_cursor.fetchall() 
        
        insert_query = """
            INSERT INTO quotes_concept (
                id, is_active, is_deleted, created_at, updated_at,deleted_at,
                title, quantity, unit_price, quote_id,
                discount_id, tax_id, rrule, event_type_id,frequency
            ) VALUES (%s, %s, %s, %s, %s,%s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,1)
            """
        if concepts : 
            for concept in concepts:
                insert_values = (
                    uuid.uuid4().hex,  
                    1, 
                    0, 
                    now,
                    now,
                    now,
                    concept.get("concept") or quotation.get("price_list_name"),
                    concept.get("quantity")or 1,
                    concept.get("unit_price")or quotation.get("total") ,
                    quote_id, 
                    None, 
                    None,  
                    None,  
                    None   
                )
                source_cursor_v2.execute(insert_query, insert_values)
        else : 
            insert_values = (
                uuid.uuid4().hex,  
                1, 
                0, 
                now,                    
                now,
                now,
                quotation.get("price_list_name") or "Servicio general",
                1,
                quotation.get("total") or 0,
                quote_id, 
                None, 
                None,  
                None,  
                None   
            )
            source_cursor_v2.execute(insert_query, insert_values)
        source_conn_v2.commit()
        


