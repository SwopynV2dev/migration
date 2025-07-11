import requests
import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()


source_conn = mysql.connector.connect(
    host="164.92.98.250",
    user="Workbench",
    password="password20@@",
    database="pwa"
)


api_url = 'https://api.pestforceapp.com/administrative/pest-control/nesting-areas/'
access_token = os.getenv("toke")
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}



try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = """SELECT * FROM pwa.areas_tree where id_area = 35;"""
        source_cursor.execute(query,())
        rows = source_cursor.fetchall()

        for row in rows:
            data = {
                
            "name": row.get('text'),
            "customer": "1d3eaa6f-1d6d-4492-b9c6-dd6d0f10e770",   
            "check_point": False,
            "certificate": False,
            "zone" : "dc393a0a-dadd-4722-bccc-943f6f790c51",
            "color": "#fffff",
            "area_category" : "7cb43e38-50be-4864-ac96-cbb97cd0b695"
            }
            response = requests.post(api_url , headers=headers, json=data)
except mysql.connector.Error as e:
    print(f"Error con la base de datos: {e}")
except requests.exceptions.RequestException as re:
    print(f"Error en la solicitud a la API: {re}")
except Exception as ex:
    print(f"Error inesperado: {ex}")

