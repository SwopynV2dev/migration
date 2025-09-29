import requests
import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()


source_conn = mysql.connector.connect(
    host=os.getenv("DB_HOST_pwa1"),
    user= os.getenv("DB_USER_pwa1")  ,
    password= os.getenv("DB_PASSWORD_pwa1")  ,
    database= os.getenv("DB_NAME_pwa1")  ,
    ssl_disabled=True
)



api_url = 'https://api.pestforceapp.com/administrative/catalogs/'
access_token = os.getenv("TOKEN")
headers = {
    'Authorization': f'Bearer {access_token}',  
    'Content-Type': 'application/json'
}

id_company_job_center =  os.getenv("UUID_ORIGINAL_pwa1")
uuid_nuevo = os.getenv("UUID_NUEVO")


try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM taxes WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        
        for row in rows :
            data = {
                "name": row.get("name"),
                "value": row.get("value"),
                "is_main" : False,
                "job_center" : uuid_nuevo
            }
            response = requests.post(api_url+"taxes/", headers=headers, json=data)
except mysql.connector.Error as e:
    print(f"Error con la tabla taxes: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla taxes: {ex}")


try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM payment_ways WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for  row in rows:
            data = {
                "name": row.get("name"),    
                "credit_days": row.get("credit_days") or 0,
                "job_center" : uuid_nuevo
            }
            response = requests.post(api_url+"payment-ways/", headers=headers, json=data)
         
except mysql.connector.Error as e:
    print(f"Error con la tabla payment_ways: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla payment_ways: {ex}")




try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM concepts WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for  row in rows:
            data = {
                "name": row.get("name"),
                "type": "EX",
                "job_center" : uuid_nuevo
            }
            response = requests.post(api_url+"concepts/", headers=headers, json=data)
         
except mysql.connector.Error as e:
    print(f"Error con la tabla concepts: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla concepts: {ex}")




try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM product_presentations WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "job_center" : uuid_nuevo
            }
            response = requests.post(api_url+"product-presentations/", headers=headers, json=data)
         
except mysql.connector.Error as e:
    print(f"Error con la tabla product_presentations: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla product_presentations: {ex}")


try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM product_types WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "key" : "VB ",
                "job_center" : uuid_nuevo
            }
            response = requests.post(api_url+"product-types/", headers=headers, json=data)
         
except mysql.connector.Error as e:
    print(f"Error con la tabla product_types: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla product_types: {ex}")




try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM product_units WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "unit": row.get("name")[0],
                "job_center" : uuid_nuevo
            }
            response = requests.post(api_url+"product-units/", headers=headers, json=data)
         
except mysql.connector.Error as e:
    print(f"Error con la tabla product_units: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla product_units: {ex}")

    


try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM discounts WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("title"),
                "description": row.get("description"),
                "percentage": row.get("percentage"),
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"discounts/", headers=headers, json=data)

except mysql.connector.Error as e:
    print(f"Error con la tabla discounts: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla discounts: {ex}")


try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM description_custom_quotes WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "description": row.get("description"),
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"custom-descriptions/", headers=headers, json=data)

except mysql.connector.Error as e:
    print(f"Error con la tabla description_custom_quotes: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla description_custom_quotes: {ex}")



try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM payment_methods WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"payment-methods/", headers=headers, json=data)

except mysql.connector.Error as e:
    print(f"Error con la tabla payment_methods: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla payment_methods: {ex}")

try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM source_origins WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"origin-sources/", headers=headers, json=data)
except mysql.connector.Error as e:
    print(f"Error con la tabla source_origins: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla source_origins: {ex}")



try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM indications WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "key": row.get("key"),
                "description": row.get("description"),
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"indications/", headers=headers, json=data)
except mysql.connector.Error as e:
    print(f"Error con la tabla indications: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla indications: {ex}")




try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM application_methods WHERE profile_job_center_id = %s"
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"application-methods-categories/", headers=headers, json=data)
         
except mysql.connector.Error as e:
    print(f"Error con la tabla application_methods: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla application_methods: {ex}")


try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = "SELECT * FROM type_areas"
        source_cursor.execute(query, ())
        rows = source_cursor.fetchall()
        for row in rows:
            data = {
                "name": row.get("name"),
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"area-categories/", headers=headers, json=data)
except mysql.connector.Error as e:
    print(f"Error con la tabla type_areas: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla type_areas: {ex}")




try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = """SELECT  pl.*, i.name AS indications_name FROM  price_lists pl LEFT JOIN 
                indications i ON pl.indications_id = i.id where pl.profile_job_center_id = %s ;"""
        source_cursor.execute(query, (id_company_job_center,))
        rows = source_cursor.fetchall()
        for row in rows:
            params = {
                "job_center": uuid_nuevo,
                "search":  row.get("indications_name")
            }
            indication = requests.get(api_url +"indications/", headers=headers,params=params)
            uuid_indication = indication.json()[0]['id']
            data = {
                "name": row.get("name"),
                "certificate_expiration_days" :  0,
                "disinfection" :  row.get("is_disinfection"),
                "show_price" : row.get("show_price"),
                "indication" : uuid_indication,
                "key" : row.get("key"),
                "comercial_conditions" :  row.get("legend"),
                "description" : row.get("description"),
                "plague" : [],
                "job_center": uuid_nuevo
            }
            response = requests.post(api_url+"service-types/", headers=headers, json=data)
except mysql.connector.Error as e:
    print(f"Error con la tabla price_lists: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla price_lists: {ex}")

source_conn.close()