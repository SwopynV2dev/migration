import pandas as pd
import requests
import traceback
from unidecode import unidecode
import re
from datetime import datetime

access_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzQ5MDU5MDM3LCJpYXQiOjE3NDEyODMwMzcsImp0aSI6IjQ4MmE0NDBkNTQzODQxYzc5ODRmZDgyM2M3OGZjNzllIiwidXNlcl9pZCI6IjM2MGRiNjAzLWYyMmItNDg5Ni1hNDBkLTI1Yjg4ZDlmNDlkOCJ9.A0eCg8_dNvzkxSiAOZse1oKEv0k1Hb0ilkpTXjWytQw'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

excel_file = 'customer.xlsx'  
business_activity_customer = 'ca627df2-f9ab-4c58-a68d-3bb2fcce491c'
uuid_job_center = '0721e548-0527-4a76-a477-af46cb300351'
api_url_customers = 'https://api.pestforceapp.com/administrative/customers/'

try:
    df = pd.read_excel(excel_file)

    grouped = df.groupby('CUENTA MADRE')
    for main_account, group in grouped:
        main_payload = {
             "name": main_account,
            "phone": 0,
            "email": 'ejemplo@gmail.com',
            "contact_name": main_account,
            "contact_phone": "",
            "contact_email": "",
            "address": "general",  
            "address_latitude": 0,
            "address_longitude": 0,
            "is_main": True,
            "main_customer": None,
            "business_activity": business_activity_customer,
            "job_center": uuid_job_center,
        }

        # Crear la cuenta madre
        response = requests.post(api_url_customers, headers=headers, json=main_payload)
        if response.status_code == 201:
            main_customer_id = response.json().get('data', {}).get('id')
            print(f"✅ Cuenta madre '{main_account}' creada con ID: {main_customer_id}")
        else:
            print(f"❌ Error al crear cuenta madre '{main_account}': {response.text}")
            continue 
        print(main_customer_id)
        # Crear subcuentas
        for _, row in group.iterrows():
            sub_payload = {
                "name": row.get('SUB-CUENTA'),
                "phone": 0,
                "email": 'ejemplo@gmail.com',
                "contact_name": row.get('SUB-CUENTA'),
                "contact_phone": "",
                "contact_email": "",
                "address": str(row.get('DIRECCION', '')),
                "address_latitude": 0,
                "address_longitude": 0,
                "is_main": False,
                "main_customer": main_customer_id,
                "business_activity": business_activity_customer,
                "job_center": uuid_job_center,
            }
            response = requests.post(api_url_customers, headers=headers, json=sub_payload)
            if response.status_code == 201:
                print(f"  - Subcuenta '{row.get('SUB-CUENTA')}' creada.")
            else:
                print(f"  ❌ Error al crear subcuenta '{row.get('SUB-CUENTA')}': {response.text}")

except Exception as e:
    print(f"❌ Error detectado: {e}")
    traceback.print_exc()

print("✅ Proceso finalizado.")
