import mysql.connector
from uuid import uuid4
from datetime import datetime
import os
from dotenv import load_dotenv

conn = mysql.connector.connect(
    host="23.21.191.62",
    user="hmcdcarlos",
    password="Canela243.",
    database="SwopynProd"
)
cursor = conn.cursor(dictionary=True)


load_dotenv()
uuid_original = os.getenv("UUID_ORIGINAL_PW2")
uuid_nuevo = os.getenv("UUID_NUEVO").replace("-", "")

products_doses_query = """
  SELECT 
    dose_type.name AS dose_name,
    dose_type.id AS dose_id,
    dose_type_prod.quantity_one,
    dose_type_prod.quantity_tow,
    product_one.name AS product_one_name,
    product_tow.name AS product_tow_name,
    unit_prod_one.name AS unit_product_one_name,
    unit_prod_two.name AS unit_product_two_name
FROM inventories_dosetypeproduct dose_type_prod
LEFT JOIN catalogs_dosetype dose_type ON dose_type_prod.dose_id = dose_type.id
LEFT JOIN inventories_product product_one ON dose_type_prod.product_one_id = product_one.id
LEFT JOIN inventories_product product_tow ON dose_type_prod.product_tow_id = product_tow.id
LEFT JOIN catalogs_unitproduct unit_prod_one ON dose_type_prod.unit_product_one_id = unit_prod_one.id
LEFT JOIN catalogs_unitproduct unit_prod_two ON dose_type_prod.unit_product_two_id = unit_prod_two.id
WHERE product_one.job_center_id = %s
"""
cursor.execute(products_doses_query, (uuid_original,))
products_doses = cursor.fetchall()

new_products_query = """
SELECT 
    dose_type_new.id AS new_dose_id,
    product_one_new.id AS new_product_one_id,
    product_tow_new.id AS new_product_tow_id,
    unit_prod_one_new.id AS new_unit_product_one_id,
    unit_prod_two_new.id AS new_unit_product_two_id
FROM catalogs_dosetype dose_type_new
LEFT JOIN inventories_product product_one_new 
    ON product_one_new.name = %s AND product_one_new.job_center_id = %s
LEFT JOIN inventories_product product_tow_new 
    ON product_tow_new.name = %s AND product_tow_new.job_center_id = %s
LEFT JOIN catalogs_unitproduct unit_prod_one_new 
    ON unit_prod_one_new.name = %s AND unit_prod_one_new.job_center_id = %s
LEFT JOIN catalogs_unitproduct unit_prod_two_new 
    ON unit_prod_two_new.name = %s AND unit_prod_two_new.job_center_id = %s
WHERE dose_type_new.name = %s AND dose_type_new.job_center_id = %s
"""

new_products_dict = {}
for entry in products_doses:
    key = (
        entry['dose_name'],
        entry['product_one_name'],
        entry['product_tow_name'],
        entry['unit_product_one_name'],
        entry['unit_product_two_name'],
    )
    cursor.execute(new_products_query, (
        entry['product_one_name'], uuid_nuevo,
        entry['product_tow_name'], uuid_nuevo,
        entry['unit_product_one_name'], uuid_nuevo,
        entry['unit_product_two_name'], uuid_nuevo,
        entry['dose_name'], uuid_nuevo
    ))
    result = cursor.fetchone()
    new_products_dict[key] = result

insert_dose_query = """
INSERT INTO inventories_dosetypeproduct (
    id, dose_id, quantity_one, quantity_tow, product_one_id, product_tow_id, 
    unit_product_one_id, unit_product_two_id, is_active, is_deleted, created_at, updated_at, deleted_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for product_data in products_doses:
    key = (
        product_data['dose_name'],
        product_data['product_one_name'],
        product_data['product_tow_name'],
        product_data['unit_product_one_name'],
        product_data['unit_product_two_name'],
    )
    new_data = new_products_dict.get(key)
    if new_data:
        new_dose_uuid = str(uuid4()).replace("-", "")
        cursor.execute(insert_dose_query, (
            new_dose_uuid,
            new_data['new_dose_id'], 
            product_data['quantity_one'],
            product_data['quantity_tow'],
            new_data['new_product_one_id'],
            new_data['new_product_tow_id'],
            new_data['new_unit_product_one_id'],
            new_data['new_unit_product_two_id'],
            True, False, datetime.now(), datetime.now(), datetime.now()  
        ))
conn.commit()
cursor.close()
conn.close()





