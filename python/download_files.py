import os
import shutil
import kagglehub

KAGGLE_DATASET_BASE_PATH = "wikimedia-foundation/wikipedia-structured-contents"
KAGGLE_EN_FILE_BASE_PATH = "enwiki/data/enwiki_namespace_0_"
KAGGLE_FR_FILE_BASE_PATH = "frwiki/data/frwiki_namespace_0_"

PER_LANGUAGE_SAMPLE_AMOUNT = 5
OUTPUT_DIR = "/datasets"

def download_kaggle_datasets():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for kaggle_language_file_path in [KAGGLE_EN_FILE_BASE_PATH, KAGGLE_FR_FILE_BASE_PATH]:
        for i in range(PER_LANGUAGE_SAMPLE_AMOUNT):
            file_name_suffix = f"{i:05}.parquet"
            kaggle_file_path = kaggle_language_file_path + file_name_suffix

            try:
                print(f"Descargando {kaggle_file_path}...")
                
                lang_prefix = "enwiki" if "enwiki" in kaggle_language_file_path else "frwiki"
                final_file_name = f"{lang_prefix}_{file_name_suffix}"
                destination_path = os.path.join(OUTPUT_DIR, final_file_name)
                
                # kagglehub descarga el archivo específico usando el parámetro path
                downloaded_path = kagglehub.dataset_download(
                    KAGGLE_DATASET_BASE_PATH, 
                    path=kaggle_file_path
                )
                
                # Manejamos si devuelve el archivo directo o una carpeta dentro de la caché
                if os.path.isfile(downloaded_path):
                    shutil.copy(downloaded_path, destination_path)
                else:
                    copied = False
                    for root, _, files in os.walk(downloaded_path):
                        for f in files:
                            if f.endswith(".parquet") and file_name_suffix in f:
                                shutil.copy(os.path.join(root, f), destination_path)
                                copied = True
                                break
                        if copied:
                            break
                            
                print(f" -> Guardado en: {destination_path}")
                
            except Exception as e:
                print(f" -> [Error] No se pudo descargar {kaggle_file_path}: {e}")

if __name__ == "__main__":
    download_kaggle_datasets()
    print("\n¡Descarga finalizada!")