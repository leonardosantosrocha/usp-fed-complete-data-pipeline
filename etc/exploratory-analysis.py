import os
os.environ['KAGGLE_USERNAME'] = ""
os.environ['KAGGLE_KEY'] = ""
import pandas as pd
import kaggle


raw_dir = "./data/01_raw"
os.makedirs(raw_dir, exist_ok=True)
dataset_name = "irs/individual-income-tax-statistics"

csv_files_present = [arquivo for arquivo in os.listdir(raw_dir) if arquivo.endswith('.csv')]

if not csv_files_present:
    print(f"Nenhum CSV encontrado em '{raw_dir}'. Baixando dataset")
    kaggle.api.dataset_download_files(
        dataset_name, 
        path=raw_dir, 
        unzip=True
    )
    print("Download concluído")
else:
    print("Pulando o download.")

csv_filename = 'field_definitions.csv'
csv_path = os.path.join(raw_dir, csv_filename)

df = pd.read_csv(csv_path)
print(df)
