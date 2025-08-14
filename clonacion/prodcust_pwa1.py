import mysql.connector
import requests
import os
from dotenv import load_dotenv


load_dotenv()

conn = mysql.connector.connect(
    host="164.92.98.250",
    user="Workbench",
    password="password20@@",
    database="pwa"
)

cursor = conn.cursor(dictionary=True)


api_url = 'https://api.pestforceapp.com/administrative/inventories/'
catalog = 'https://api.pestforceapp.com/administrative/catalogs/'
access_token = os.getenv("TOKEN")
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}    

uuid_original = os.getenv("UUID_ORIGINAL_pwa1")
uuid_nuevo = os.getenv("UUID_NUEVO")



suplir_query = 'SELECT * FROM pwa.providers WHERE profile_job_center_id = %s'
cursor.execute(suplir_query, (uuid_original,))
suplirs = cursor.fetchall()


for row in suplirs :
    data =  {
        "name": row.get("company"),
        "contact_name": row.get("contact_name"),
        "address": row.get("contact_address"),
        "phone": row.get("contact_cellphone"),
        "email": row.get("contact_email"),
        "bank": row.get("bank"), 
        "account_holder": row.get("account_holder"),
        "account_number": row.get("account_number"),
        "taxpayer_registration": row.get("taxpayer_registration"),
        "job_center": uuid_nuevo
    }
    response = requests.post(api_url+"suppliers/", headers=headers, json=data)


products_query = """
SELECT 
    p.*,        
    pr.name AS presentation_name,
    tp.name AS type_product_name,
    up.name AS unit_product_name,
    t.name AS tax_name
FROM 
    products p
LEFT JOIN 
    product_presentations pr ON p.id_presentation = pr.id
LEFT JOIN 
    product_types tp ON p.id_type_product = tp.id
LEFT JOIN 
    product_units up ON p.id_unit = up.id
LEFT JOIN 
    product_tax pt ON pt.id_product = p.id
LEFT JOIN 
    taxes t ON pt.id_tax = t.id
WHERE 
    p.profile_job_center_id = %s;

"""
cursor.execute(products_query, (uuid_original,))
products = cursor.fetchall()


for product in products : 

    params = {"job_center": uuid_nuevo, "search": product.get("type_product_name")}
    type_response = requests.get(catalog + "product-types/", headers=headers, params=params)
    type_data = type_response.json()
    type_p = type_data[0]['id'] if type_data else None

    params = {"job_center": uuid_nuevo, "search": product.get("tax_name")}
    type_response = requests.get(catalog + "taxes/", headers=headers, params=params)
    type_data = type_response.json()
    type_p = type_data[0]['id'] if type_data else None

    params = {"job_center": uuid_nuevo, "search": product.get("type_product_name")}
    type_response = requests.get(catalog + "product-types/", headers=headers, params=params)
    type_data = type_response.json()
    type_p = type_data[0]['id'] if type_data else None

    params["search"] = product.get("presentation_name")
    presentation_response = requests.get(catalog + "product-presentations/", headers=headers, params=params)
    presentation_data = presentation_response.json()
    presentation_p = presentation_data[0]['id'] if presentation_data else None


    params["search"] = product.get("unit_product_name")
    unit_response = requests.get(catalog + "product-units/", headers=headers, params=params)
    unit_data = unit_response.json()
    unit_p = unit_data[0]['id'] if unit_data else None
    data = {
        "name": product.get("name"),
        "type_product": type_p,
        "description": product.get("description"),
        "features": product.get("features"),
        "suggested_use": product.get("suggested_use"),
        "presentation": presentation_p,
        "unit_product": unit_p,
        "quantity": product.get("quantity"),
        "ingredient": product.get("ingredient"),
        "pesticide_registration": product.get("pesticide_registration"),
        "price": product.get("price"),
        "sale_price": product.get("sale_price"),
        "image": None,
        "is_available": product.get("is_available"),
        "job_center": uuid_nuevo,
        "tax":  product.get("tax"),
        "supplier": [],
        "stock_min":0,
        "stock_max" : 1000,
        "tax" :[]   
    }
    response = requests.post(api_url + "products/", headers=headers, json=data)  




