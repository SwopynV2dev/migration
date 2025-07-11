import requests
import mysql.connector
import os
from dotenv import load_dotenv

source_conn = mysql.connector.connect(
    host="164.92.98.250",
    user="Workbench",
    password="password20@@",
    database="pwa"
)


api_url = 'https://api.pestforceapp.com/administrative/employees/'

access_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzU2ODM1MjM3LCJpYXQiOjE3NDkwNTkyMzcsImp0aSI6IjFmYmZiY2E1NGIxMzQwMjU5YTllNmI5YTQ3MWZlN2UzIiwidXNlcl9pZCI6IjVmNGVjNWVmLWVhYTktNDczMS1iZjdlLTdlZWNmZGEwYzg5MiJ9.az5gY9lk8mb-h87d16ep9ZmloxqyNeKko2rNGJb61qM'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

load_dotenv()
id_company = os.getenv("UUID_ORIGINAL_pwa1")
uuid_nuevo = os.getenv("UUID_NUEVO")
uuid_job_title = os.getenv("UUID_JOB_TITLE")

try:
    with source_conn.cursor(dictionary=True) as source_cursor:
        query = """SELECT 
    u.*,
    e.name as employe_rea
FROM 
    pwa_prod.users u
JOIN 
    pwa.employees e 
ON 
    u.id = e.employee_id
WHERE 
     e.profile_job_center_id = %s;"""
        source_cursor.execute(query, (id_company,))
        rows = source_cursor.fetchall() 
        for row in rows :
            data = {
                "name": row.get("employe_rea"),
                "job_title": uuid_job_title,
                "color" : "#3C589",
                "is_active" : True,
                "management" : "Gerencia",
                "payroll_number" : "1",
                "location" : "",
                "zone" : "",
                "job_center" : uuid_nuevo
            }

            employee = requests.post(api_url, headers=headers, json=data)
            data = {
                "username": row.get("email"),
                "password": "admin243",
                "role" : "DL",
            }
            user = requests.post(api_url+employee.json()['id']+"/create_account/", headers=headers, json=data)
except mysql.connector.Error as e:
    print(f"Error con la tabla taxes: {e}")
except Exception as ex:
    print(f"Error inesperado con la tabla taxes: {ex}")
