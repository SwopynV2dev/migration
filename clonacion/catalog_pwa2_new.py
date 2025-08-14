import mysql.connector
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv  

conn = mysql.connector.connect(
    host="23.21.191.62",
    user="hmcdcarlos",
    password="Canela243.",
    database="SwopynProd",
    ssl_disabled=True
)

load_dotenv()
uuid_original = os.getenv("UUID_ORIGINAL_PW2")  
uuid_nuevo = os.getenv("UUID_NUEVO")


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_applicationmethodcategory
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:    
            new_id = uuid.uuid4().hex  
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_applicationmethodcategory (
                    id, name, job_center_id, is_active, is_deleted,
                    created_at, updated_at,deleted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id.replace("-",""),row["name"],uuid_nuevo.replace("-", ""),
                1,0,now,now,now  ))
            conn.commit()




with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_cancellationreason
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex  
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_cancellationreason (
                    id, name, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id.replace("-",""),row["name"],uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))

            conn.commit()




with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_cleaningcategory
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_cleaningcategory (
                    id, name, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id.replace("-",""), row["name"],uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))

            conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, `key`
        FROM catalogs_typeproduct
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex  
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_typeproduct (
                    id, name, `key`, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id.replace("-",""),row["name"],row["key"],uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))
            conn.commit()

with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, description
        FROM catalogs_customdescription
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_customdescription (
                    id, name, description, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id.replace("-",""),row["name"],row["description"],uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))

            conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, description, percentage
        FROM catalogs_discount
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_discount (
                    id, name, description, percentage, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id,row["name"],row["description"],row["percentage"],
                uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))
            conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, description, quantity
        FROM catalogs_extra
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_extra (
                    id, name, description, quantity, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id,row["name"], row["description"],row["quantity"], uuid_nuevo.replace("-", ""),
                1,0,now,now,now
            ))
            conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, `key`, description
        FROM catalogs_indication
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()
    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_indication (
                    id, name, `key`, description, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id.replace("-",""),row["name"],row["key"],row["description"],
                uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))

            conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, `key`, description
        FROM catalogs_indication
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_indication (
                    id, name, `key`, description, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id,row["name"],row["key"],row["description"],
                uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))

            conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_originsource
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_originsource (
                    id, name, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id, row["name"], uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))

            conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_presentation
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor() as insert_cursor:
        for row in rows:
            new_id = uuid.uuid4().hex
            now = datetime.now()

            insert_query = """
                INSERT INTO catalogs_presentation (
                    id, name, job_center_id,
                    is_active, is_deleted,
                    created_at, updated_at,deleted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            """
            insert_cursor.execute(insert_query, (
                new_id,row["name"], uuid_nuevo.replace("-", ""),1,0,now,now,now
            ))

            conn.commit()




with conn.cursor(dictionary=True) as source_cursor:
        query = """
            SELECT * FROM SwopynProd.catalogs_tax
            WHERE job_center_id = %s
        """
        source_cursor.execute(query, (uuid_original,))
        tax_records = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for record in tax_records:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO SwopynProd.catalogs_tax
            (id, is_active, is_deleted, created_at, updated_at, deleted_at,
                name, value, is_main, job_center_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            int(record.get("is_active", 1)),
            int(record.get("is_deleted", 0)),
            now,
            now,
            now,
            record.get("name"),
            record.get("value"),
            int(record.get("is_main", 0)),
            uuid_nuevo.replace("-", "")
        ))

conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, `key`
        FROM catalogs_typeproduct
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_typeproduct (
                id, name, `key`, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,row["name"],row["key"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name,  `key`
        FROM catalogs_typeproduct
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_typeproduct (
                id, name,  `key`, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id, row["name"], row["key"],
            uuid_nuevo.replace("-", ""), 1, 0, now, now, now
        ))
        conn.commit()




with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_voucher
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_voucher (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))
        conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_dosetype
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_dosetype (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, folio_key_setting, folio_init_setting, 
               days_number_validity, visit_number_no_incident
        FROM catalogs_tickettype
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_tickettype (
                id, name, folio_key_setting, folio_init_setting, 
                job_center_id, is_active, is_deleted, 
                created_at, updated_at, deleted_at,
                days_number_validity, visit_number_no_incident
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            row.get("folio_key_setting", ""),
            row.get("folio_init_setting", 0),
            uuid_nuevo.replace("-", ""),
            1, 0, now, now, now,
            row.get("days_number_validity", 0),
            row.get("visit_number_no_incident", 0)
        ))

        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_media
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_media (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()






with conn.cursor(dictionary=True) as source_cursor:
        query = """
            SELECT * FROM SwopynProd.catalogs_rejectionreason
            WHERE job_center_id = %s
        """
        source_cursor.execute(query, (uuid_original,))
        rejection_records = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for record in rejection_records:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO SwopynProd.catalogs_rejectionreason
            (id, is_active, is_deleted, created_at, updated_at, deleted_at,
                name, job_center_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            int(record.get("is_active", 1)),
            int(record.get("is_deleted", 0)),
            now,
            now,
            now,
            record.get("name"),
            uuid_nuevo.replace("-", "")
        ))

conn.commit()





with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_areacategory
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_areacategory (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, folio_key_setting, folio_init_setting, customer_field, nesting_area_field, administrative_user, client_user
        FROM catalogs_tasktype
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_tasktype (
                id, name, folio_key_setting, folio_init_setting,
                customer_field, nesting_area_field, administrative_user,
                client_user, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            row["folio_key_setting"],
            row["folio_init_setting"],
            row["customer_field"],
            row["nesting_area_field"],
            row["administrative_user"],
            row["client_user"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_typedocumentproduct
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_typedocumentproduct (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, value
        FROM catalogs_infestationdegree
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_infestationdegree (
                id, name, value, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            row["value"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()





with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, activity_field, conditions_field, observations_field,
               corrective_actions_field, species_count_field, is_catch_field
        FROM catalogs_stationtype
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_stationtype (
                id, name, activity_field, conditions_field, observations_field,
                corrective_actions_field, species_count_field, is_catch_field,
                job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            row["activity_field"],
            row["conditions_field"],
            row["observations_field"],
            row["corrective_actions_field"],
            row["species_count_field"],
            row["is_catch_field"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()




with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_stationactivity
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_stationactivity (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_stationcondition
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_stationcondition (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_qualification
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_qualification (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_rootcause
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_rootcause (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()




with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_paymentmethod
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_paymentmethod (
                id, name, job_center_id,
                is_active, is_deleted,
                created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name, credit_days 
        FROM catalogs_paymentway 
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_paymentway (
                id, name, credit_days, job_center_id, 
                is_active, is_deleted, created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            row["credit_days"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name 
        FROM catalogs_activityarea 
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_activityarea (
                id, name, job_center_id, 
                is_active, is_deleted, created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()

with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT name
        FROM catalogs_conditionalareas
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO catalogs_conditionalareas (
                id, name, job_center_id,
                is_active, is_deleted, created_at, updated_at, deleted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row["name"],
            uuid_nuevo.replace("-", ""),1,0,now,now,now
        ))

        conn.commit()

            
with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT * FROM catalogs_eventtype
        WHERE job_center_id = %s AND is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for row in rows:
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
           INSERT INTO catalogs_eventtype (
    id, name, warranty, follow, quote_field, customer_field,
    employee_field, service_type_field, plagues_field, cost_field,
    comments_field, ticket_field, group_field, mip_inspection_form,
    mip_condition_form, mip_control_form, mip_payment_form, mip_signature_form,
    mip_plan_action_form, mip_station_count_form, mip_task_form, mip_AreasActivity,
    notification_action, reminder_action, folio_key_setting, folio_init_setting,
    hide_cancel_setting, is_service_order, is_follow, is_warranty,
    booster, is_bootster,
    job_center_id, is_active, is_deleted, created_at, updated_at, deleted_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s)

        """
        insert_cursor.execute(insert_query, (
            new_id,
            row.get("name"),
            row.get("warranty"),
            row.get("follow"),
            row.get("quote_field"),
            row.get("customer_field"),
            row.get("employee_field"),
            row.get("service_type_field"),
            row.get("plagues_field"),
            row.get("cost_field"),
            row.get("comments_field"),
            row.get("ticket_field"),
            row.get("group_field"),
            row.get("mip_inspection_form"),
            row.get("mip_condition_form"),
            row.get("mip_control_form"),
            row.get("mip_payment_form"),
            row.get("mip_signature_form"),
            row.get("mip_plan_action_form"),
            row.get("mip_station_count_form"),
            row.get("mip_task_form"),
            row.get("mip_AreasActivity"),
            row.get("notification_action"),
            row.get("reminder_action"),
            row.get("folio_key_setting"),
            row.get("folio_init_setting"),
            row.get("hide_cancel_setting"),
            row.get("is_service_order"),
            row.get("is_follow"),
            row.get("is_warranty"),
            row.get("booster"),
            row.get("is_bootster"),
            uuid_nuevo.replace("-", ""),
            1,
            0,
            now,
            now,
            now
        ))
        conn.commit()



with conn.cursor(dictionary=True) as source_cursor, conn.cursor() as insert_cursor:
    source_cursor.execute("""
        SELECT * FROM SwopynProd.catalogs_plaguecategory 
        WHERE job_center_id = %s AND is_deleted = FALSE
    """, (uuid_original,))
    plague_categories = source_cursor.fetchall()
    for category in plague_categories:
        insert_cursor.execute("""
            INSERT INTO SwopynProd.catalogs_plaguecategory (
                id, name, job_center_id, is_active, is_deleted, created_at, updated_at, deleted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            uuid.uuid4().hex, category['name'], uuid_nuevo.replace("-", ""),
            category['is_active'], category['is_deleted'],
            category['created_at'], category['updated_at'], category['deleted_at']
        ))
        conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
        query = """
            SELECT cup.*, ctp.name AS type_product_name
            FROM SwopynProd.catalogs_unitproduct cup
            LEFT JOIN SwopynProd.catalogs_typeproduct ctp
                ON cup.type_product_id = ctp.id
            WHERE cup.job_center_id = %s
        """
        source_cursor.execute(query, (uuid_original,))
        unit_product_records = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for record in unit_product_records:
        new_type_product_id = None
        if record.get("type_product_name"):
            query_new_type = """
                SELECT id FROM SwopynProd.catalogs_typeproduct
                WHERE job_center_id = %s AND name = %s
            """
            with conn.cursor(dictionary=True) as lookup_cursor:
                lookup_cursor.execute(query_new_type, (
                    uuid_nuevo.replace("-", ""),
                    record['type_product_name']
                ))
                rows = lookup_cursor.fetchall()
                if rows:
                    new_type_product_id = rows[0]['id']
            

        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO SwopynProd.catalogs_unitproduct
            (id, is_active, is_deleted, created_at, updated_at, deleted_at,
                name, job_center_id, unit, type_product_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            int(record.get("is_active", 1)),
            int(record.get("is_deleted", 0)),
            now,
            now,
            now,
            record.get("name"),
            uuid_nuevo.replace("-", ""),
            record.get("unit"),
            new_type_product_id
        ))

conn.commit()





with conn.cursor(dictionary=True) as source_cursor:
    query_evidences = """
        SELECT ce.*, cpc.name AS plague_category_name
        FROM SwopynProd.catalogs_evidencetype ce
        LEFT JOIN SwopynProd.catalogs_plaguecategory cpc 
            ON ce.plague_category_id = cpc.id
        WHERE ce.job_center_id = %s
    """
    source_cursor.execute(query_evidences, (uuid_original,))
    evidence_records = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for record in evidence_records:
        new_plague_category_id = None

        if record['plague_category_name']:
            query_new_category = """
                SELECT id FROM SwopynProd.catalogs_plaguecategory 
                WHERE job_center_id = %s AND name = %s
            """
            with conn.cursor(dictionary=True) as lookup_cursor:
                lookup_cursor.execute(query_new_category, (uuid_nuevo.replace("-", ""), record['plague_category_name']))
                rows = lookup_cursor.fetchall()
                new_category = rows[0] if rows else None

            if new_category:
                new_plague_category_id = new_category['id']
      

        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO SwopynProd.catalogs_evidencetype
            (id, is_active, is_deleted, created_at, updated_at, deleted_at, name, job_center_id, plague_category_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            int(record.get("is_active", 1)),
            int(record.get("is_deleted", 0)),
            now,
            now,
            now,
            record.get("name"),
            uuid_nuevo.replace("-", ""),
            new_plague_category_id
        ))

conn.commit()

with conn.cursor(dictionary=True) as source_cursor:
    query_plagues = """
        SELECT cp.*, cpc.name AS category_name
        FROM SwopynProd.catalogs_plague cp
        LEFT JOIN SwopynProd.catalogs_plaguecategory cpc 
            ON cp.category_id = cpc.id
        WHERE cp.job_center_id = %s
    """
    source_cursor.execute(query_plagues, (uuid_original,))
    plague_records = source_cursor.fetchall()

with conn.cursor() as insert_cursor:
    for record in plague_records:
        new_category_id = None

        query_new_category = """
            SELECT id FROM SwopynProd.catalogs_plaguecategory 
            WHERE job_center_id = %s AND name = %s
        """
        with conn.cursor(dictionary=True) as lookup_cursor:
            lookup_cursor.execute(query_new_category, (uuid_nuevo.replace("-", ""), record['category_name']))
            rows = lookup_cursor.fetchall()
            new_category = rows[0] if rows else None

        if new_category is None:
            print(f"⚠️ No se encontró category '{record['category_name']}' para el nuevo job_center.")
            continue

        new_category_id = new_category['id']
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO SwopynProd.catalogs_plague
            (id, is_active, is_deleted, created_at, updated_at, deleted_at, name, category_id, job_center_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            int(record.get("is_active", 1)),
            int(record.get("is_deleted", 0)),
            now,
            now,
            now,
            record.get("name"),
            new_category_id,
            uuid_nuevo.replace("-", "")
        ))

conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT cc.*, 
            ccc.name AS category_name, 
            ctt.name AS type_task_name
        FROM SwopynProd.catalogs_cleaning cc
        INNER JOIN SwopynProd.catalogs_cleaningcategory ccc ON cc.category_id = ccc.id
        INNER JOIN SwopynProd.catalogs_tasktype ctt ON cc.type_task_id = ctt.id
        WHERE cc.job_center_id = %s AND cc.is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

with conn.cursor(dictionary=True) as lookup_cursor, conn.cursor() as insert_cursor:
    for row in rows:
        query_category = """
            SELECT id FROM SwopynProd.catalogs_cleaningcategory 
            WHERE name = %s AND job_center_id = %s AND is_deleted = FALSE
        """
        lookup_cursor.execute(query_category, (row['category_name'], uuid_nuevo))
        new_category = lookup_cursor.fetchone()
        lookup_cursor.fetchall()
        new_category_id = new_category['id'] if new_category else None

        query_type_task = """
            SELECT id FROM SwopynProd.catalogs_tasktype 
            WHERE name = %s AND job_center_id = %s AND is_deleted = FALSE
        """
        lookup_cursor.execute(query_type_task, (row['type_task_name'], uuid_nuevo))
        new_type_task = lookup_cursor.fetchone()
        lookup_cursor.fetchall()
        new_type_task_id = new_type_task['id'] if new_type_task else None

        new_id = uuid.uuid4().hex
        now = datetime.now()
        insert_query = """
            INSERT INTO SwopynProd.catalogs_cleaning (
                id, name, aplicate_suggestion, description, incidents, name_task, suggestion,
                category_id, type_task_id, job_center_id, is_active, is_deleted,
                created_at, updated_at, deleted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row.get("name"),
            row.get("aplicate_suggestion"),
            row.get("description"),
            row.get("incidents"),
            row.get("name_task"),
            row.get("suggestion"),
            new_category_id,
            new_type_task_id,
            uuid_nuevo.replace("-", ""),
            1,
            0,
            now,
            now,
            now
        ))

    conn.commit()







with conn.cursor(dictionary=True) as source_cursor:
    query_lifecycle = """
        SELECT clpc.*, cpc.name AS plague_category_name
        FROM SwopynProd.catalogs_lifecycleplaguecategory clpc
        LEFT JOIN SwopynProd.catalogs_plaguecategory cpc 
            ON clpc.plague_category_id = cpc.id
        WHERE clpc.job_center_id = %s
    """
    source_cursor.execute(query_lifecycle, (uuid_original,))
    lifecycle_records = source_cursor.fetchall()

# 💡 Mueve el resto del código FUERA del `with conn.cursor(...)` anterior.
with conn.cursor() as insert_cursor:
    for record in lifecycle_records:
        new_plague_category_id = None

        if record['plague_category_name']:
            query_new_plague_category = """
                SELECT id FROM SwopynProd.catalogs_plaguecategory 
                WHERE job_center_id = %s AND name = %s
            """
            with conn.cursor(dictionary=True) as lookup_cursor:
                lookup_cursor.execute(query_new_plague_category, (uuid_nuevo.replace("-",""), record['plague_category_name']))
                rows = lookup_cursor.fetchall()
                new_plague_category = rows[0] if rows else None


            if new_plague_category:
                new_plague_category_id = new_plague_category['id']
         
        new_id = uuid.uuid4().hex
        now = datetime.now()

        insert_query = """
            INSERT INTO SwopynProd.catalogs_lifecycleplaguecategory
            (id, name, is_active, is_deleted, job_center_id, plague_category_id, created_at, updated_at, deleted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            record.get("name"),
            int(record.get("is_active", 1)),
            int(record.get("is_deleted", 0)),
            uuid_nuevo.replace("-", ""),
            new_plague_category_id,
            now,
            now,
            now
        ))

conn.commit()





with conn.cursor(dictionary=True) as cursor:
    query = """
    SELECT cam.*, camc.name AS category_name
    FROM catalogs_applicationmethod cam
    LEFT JOIN catalogs_applicationmethodcategory camc ON cam.category_id = camc.id
    WHERE cam.job_center_id = %s
    """
    cursor.execute(query, (uuid_original,))
    rows = cursor.fetchall()

for row in rows:
    category_name = row.get("category_name")
    new_category_id = None

    with conn.cursor(dictionary=True, buffered=True) as inner_cursor:
        inner_cursor.execute("""
            SELECT id FROM catalogs_applicationmethodcategory
            WHERE job_center_id = %s AND name = %s
        """, (uuid_nuevo.replace("-", ""), category_name))
        result = inner_cursor.fetchone()


    new_category_id = result["id"]
    new_id = uuid.uuid4().hex

    with conn.cursor() as insert_cursor:
        insert_query = """
            INSERT INTO catalogs_applicationmethod 
            (id, name, job_center_id, category_id, is_active, is_deleted, created_at, updated_at, deleted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        insert_cursor.execute(insert_query, (
            new_id,
            row.get("name"),
            uuid_nuevo.replace("-", ""),
            new_category_id,
            row.get("is_active"),
            row.get("is_deleted"),
            row.get("created_at"),
            row.get("updated_at"),
            row.get("deleted_at"),
        ))

conn.commit()



with conn.cursor(dictionary=True) as source_cursor:
    query = """
    SELECT csta.*, cst.name AS station_type_name
    FROM catalogs_stationtypeactivity csta
    LEFT JOIN catalogs_stationtype cst ON csta.station_type_id = cst.id
    WHERE csta.job_center_id = %s AND csta.is_deleted = FALSE
    """
    source_cursor.execute(query, (uuid_original,))
    rows = source_cursor.fetchall()

    with conn.cursor(dictionary=True) as lookup_cursor:
        
        for row in rows:
            lookup_cursor.execute("""
            SELECT name, id FROM catalogs_stationtype
            WHERE job_center_id = %s and name = %s
            """,(uuid_nuevo.replace("-", ""),row.get('station_type_name'),))
            station_type_map = {row['name']: row['id'] for row in lookup_cursor.fetchall()}
            station_type_name = row.get("station_type_name")
            new_station_type_id = station_type_map.get(station_type_name)
            new_id = uuid.uuid4().hex
            lookup_cursor.execute("""
                INSERT INTO catalogs_stationtypeactivity (
                    id, name, job_center_id, station_type_id, is_active, is_deleted, created_at, updated_at, deleted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                new_id,
                row.get("name"),
                uuid_nuevo.replace("-", ""),
                new_station_type_id,
                row.get("is_active"),
                row.get("is_deleted"),
                row.get("created_at"),
                row.get("updated_at"),
                row.get("deleted_at"),
            ))

        conn.commit()


with conn.cursor(dictionary=True) as source_cursor:
    query_service_type = """
    SELECT 
        cs.*,
        ci.name AS indication_name,
        ccd.name AS customer_description_name,
        GROUP_CONCAT(cp.name SEPARATOR ', ') AS plague_names 
    FROM SwopynProd.catalogs_servicetype cs	
    INNER JOIN SwopynProd.catalogs_indication ci ON cs.indication_id = ci.id
    INNER JOIN SwopynProd.catalogs_customdescription ccd ON cs.customerDescription_id = ccd.id
    LEFT JOIN SwopynProd.catalogs_servicetype_plague csp ON cs.id = csp.servicetype_id  
    LEFT JOIN SwopynProd.catalogs_plague cp ON csp.plague_id = cp.id  
    WHERE cs.job_center_id = %s and cs.is_deleted = FALSE
    GROUP BY cs.id;
    """
    source_cursor.execute(query_service_type, (uuid_original,))
    service_types = source_cursor.fetchall()
    for service in service_types:
        source_cursor.execute("""
            SELECT id FROM catalogs_indication 
            WHERE job_center_id = %s AND name = %s
        """, (uuid_nuevo.replace("-", ""), service['indication_name']))
        indication_res = source_cursor.fetchone()
        source_cursor.fetchall()
        new_indication_id = indication_res['id'] if indication_res else None

        source_cursor.execute("""
            SELECT id FROM catalogs_customdescription 
            WHERE job_center_id = %s AND name = %s
        """, (uuid_nuevo.replace("-", ""), service['customer_description_name']))
        customdesc_res = source_cursor.fetchone()
        new_customer_description_id = customdesc_res['id'] if customdesc_res else None
        source_cursor.fetchall()

        plague_ids = []
        if service['plague_names']:
            plague_names = [name.strip() for name in service['plague_names'].split(',')]
            for pname in plague_names:
                source_cursor.execute("""
                    SELECT id FROM catalogs_plague
                    WHERE job_center_id = %s AND name = %s
                """, (uuid_nuevo.replace("-", ""), pname))
                plague_res = source_cursor.fetchone()
                source_cursor.fetchall()
                if plague_res:
                    plague_ids.append(plague_res['id'])



        new_id = uuid.uuid4().hex

        source_cursor.execute("""
            INSERT INTO catalogs_servicetype (
                id, is_active, is_deleted, created_at, updated_at, deleted_at,
                name, frequency_days, certificate_expiration_days, follow_up_days,
                disinfection, show_price, cover, indication_id, job_center_id,
                comercial_conditions, description, customerDescription_id, `key`, agent
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """, (
            new_id,
            1,  # is_active
            0,  # is_deleted
            datetime.now(),  # created_at
            datetime.now(),  # updated_at
            datetime.now(),  # deleted_at
            service.get("name"),
            service.get("frequency_days"),
            service.get("certificate_expiration_days"),
            service.get("follow_up_days"),
            bool(service.get("disinfection")),
            bool(service.get("show_price")),
            service.get("cover") or "",  # cover
            new_indication_id,
            uuid_nuevo.replace("-", ""),
            service.get("comercial_conditions"),
            service.get("description"),
            new_customer_description_id,
            service.get("key"),
            service.get("agent") or ""  # agent
        ))


        for plague_id in plague_ids:
            source_cursor.execute("""
                INSERT INTO catalogs_servicetype_plague (servicetype_id, plague_id)
                VALUES (%s, %s)
            """, (new_id, plague_id))
        source_cursor.execute("""
            SELECT scale, price, price_reforce 
            FROM catalogs_price_lis 
            WHERE service_type_id = %s
        """, (service['id'],))
        price_list = source_cursor.fetchall()
        for price in price_list:
            source_cursor.execute("""
                INSERT INTO catalogs_price_lis (service_type_id, scale, price, price_reforce)
                VALUES (%s, %s, %s, %s)
            """, (new_id, price['scale'], price['price'], price['price_reforce']))

        conn.commit()






search = uuid_nuevo.replace("-", "")

with conn.cursor(dictionary=True) as source_cursor:
    query = """
        SELECT * FROM catalogs_businessactivity 
        WHERE job_center_id = %s AND is_deleted = 0
    """
    source_cursor.execute(query, (uuid_original,))
    bussines = source_cursor.fetchall()

    for bussine in bussines:
        insert_query = """
            INSERT INTO catalogs_businessactivity (
                id, is_active, is_deleted, created_at, updated_at, deleted_at, 
                name, job_center_id, business_activity_icon_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        bussines_id = str(uuid.uuid4()).replace("-", "")
        values_bussines = (
            bussines_id, 1, 0, datetime.now(), datetime.now(), datetime.now(),
            bussine.get("name"), search, bussine.get("business_activity_icon_id")
        )
        source_cursor.execute(insert_query, values_bussines)
        conn.commit()  # Confirmar inserción

        query_activity = """
            SELECT * FROM business_pest_zone 
            WHERE business_activity_id = %s
        """
        source_cursor.execute(query_activity, (bussine.get('id'),))
        zones = source_cursor.fetchall()

        for zone in zones:
            insert_query = """
                INSERT INTO business_pest_zone (
                    id, is_active, is_deleted, created_at, updated_at, deleted_at, 
                    name, business_activity_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            zone_id = str(uuid.uuid4()).replace("-", "")
            values_zone = (
                zone_id, 1, 0, datetime.now(), datetime.now(), datetime.now(),
                zone.get("name"), bussines_id
            )
            source_cursor.execute(insert_query, values_zone)
            conn.commit()

            query_nesting = """
                SELECT 
                    bpn.*, ac.name AS area_category_name, ac.id AS area_category_id
                FROM SwopynProd.business_pest_nestingarea AS bpn
                JOIN SwopynProd.catalogs_areacategory AS ac
                ON bpn.area_category_id = ac.id 
                WHERE zone_id = %s
            """
            source_cursor.execute(query_nesting, (zone.get('id'),))
            areas = source_cursor.fetchall()

            for area in areas:
                query_nesting = """
                    SELECT id FROM SwopynProd.catalogs_areacategory 
                    WHERE job_center_id = %s AND is_deleted = 0 AND name = %s
                """
                source_cursor.execute(query_nesting, (search, area.get("area_category_name")))
                area_category = source_cursor.fetchone()
                source_cursor.fetchall()

                if area_category:
                    insert_query = """
                        INSERT INTO business_pest_nestingarea (
                            id, is_active, is_deleted, created_at, updated_at, deleted_at, 
                            name, check_point, certificate, color, area_category_id, 
                            business_activity_id, zone_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    nesting_id = str(uuid.uuid4()).replace("-", "")
                    values_nesting = (
                        nesting_id, 1, 0, datetime.now(), datetime.now(), datetime.now(),
                        area.get("name"),
                        area.get("check_point") if area.get("check_point") is not None else 0,
                        area.get("certificate") if area.get("certificate") is not None else 0,
                        area.get("color"), area_category["id"], bussines_id, zone_id
                    )
                    source_cursor.execute(insert_query, values_nesting)
                    conn.commit()

conn.close()