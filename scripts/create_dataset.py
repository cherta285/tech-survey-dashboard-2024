# Создайте файл scripts/create_dataset.py
from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))

dataset_id = f"{client.project}.tech_survey_data"
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"

dataset = client.create_dataset(dataset, exists_ok=True)
print(f"✓ Dataset {dataset_id} создан")