import os
from io import BytesIO
import boto3
import qrcode
import qrcode.image.svg
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML, CSS
import traceback
from dotenv import load_dotenv

load_dotenv() 


aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_SES_REGION')

client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
STATIC_DIR = os.path.join(BASE_DIR, 'static')

env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


def create_pdf(template_name: str, context: dict) -> bytes:
    template = env.get_template(template_name)
    html_template = template.render(context)

    custom_css = """
    p {
        color: #333; 
    }   
    .hide-numbers span {
        display: none;
    }
    """
    css_path = os.path.join(TEMPLATE_DIR, 'bootstrap.min.css')

    pdf_bytes = HTML(string=html_template, base_url=BASE_DIR).write_pdf(
        stylesheets=[
            CSS(filename=css_path),
            CSS(string=custom_css)
        ]
    )
    return pdf_bytes


    
def upload_pdf(pdf: object, key: str) -> str:
    client = boto3.client('s3')
    bucket_name = 'pestware-storage-devs'
    client.delete_object(Bucket=bucket_name, Key=key)
    response = client.put_object(
        Bucket=bucket_name,
        ContentType='application/pdf',
        Body=pdf,
        Key=key,
        CacheControl='no-cache, no-store, must-revalidate'
    )


