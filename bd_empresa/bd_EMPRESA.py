from datetime import date
import dotenv
from pathlib import Path
from sqlalchemy import create_engine
import bs4 as bs
import ftplib
import gzip
import os
import pandas as pd
import psycopg2
import re
import sys
import time
import urllib.request
import wget
import zipfile

def getEnv(env):
    return os.getenv(env)
dotenv.load_dotenv()

output_files = Path(getEnv('OUTPUT_FILES_PATH'))
extracted_files = Path(getEnv('EXTRACTED_FILES_PATH'))

#print('Descompactando arquivo:')
#with zipfile.ZipFile(OUTPUT_FILES_PATH) as zip_ref:
 #   zip_ref.extractall(extracted_files)
    

insert_start = time.time()
extracted_files = Path(getEnv('EXTRACTED_FILES_PATH'))
# Files:
Items = [name for name in os.listdir(extracted_files) if name.endswith('')]

# Separar arquivos:
arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []

for i in range(len(Items)):
    if Items[i].find('EMPRE') > -1:
        arquivos_empresa.append(Items[i])
    elif Items[i].find('ESTABELE') > -1:
        arquivos_estabelecimento.append(Items[i])
    elif Items[i].find('SOCIO') > -1:
        arquivos_socios.append(Items[i])
    else:
        pass

# Conectar no banco de dados:
# Dados da conexão com o BD
user=getEnv('DB_USER')
passw=getEnv('DB_PASSWORD')
host=getEnv('DB_HOST')
port=getEnv('DB_PORT')
database=getEnv('DB_NAME')

# Conectar:
engine = create_engine('postgresql://'+user+':'+passw+'@'+host+':'+port+'/'+database)
conn = psycopg2.connect('dbname='+database+' '+'user='+user+' '+'host='+host+' '+'password='+passw)
cur = conn.cursor()

empresa_insert_start = time.time()
print("""
#######################
## Arquivos de EMPRESA:
#######################
""")

#Dropping tabela antes de inserí-la
cur.execute('DROP TABLE IF EXISTS "empresa";')
conn.commit()

for e in range(0, len(arquivos_empresa)):
    print('Trabalhando no arquivo: '+arquivos_empresa[e]+' [...]')
    try:
        del empresa
    except:
        pass

    empresa = pd.DataFrame(columns=[0, 1, 2, 3, 4, 5, 6])
    empresa_dtypes = {0: 'object', 1: 'object', 2: 'object', 3: 'object', 4: 'object', 5: 'object', 6: 'object'}
    extracted_file_path = Path(f'{extracted_files}/{arquivos_empresa[e]}')

    empresa = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep = ';',
                          engine='python',
                          encoding= 'cp1252',
                          #nrows=100,
                          skiprows=0,
                          header=None,
                          dtype=empresa_dtypes)

    #Tratamento do arquivo antes de inserir na base:

    #Excluindo coluna index
    empresa = empresa.reset_index()
    del empresa['index']

    #Renomeando colunas conforme o layout
    empresa.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

    
    #Gravação de tabela transformada no dados no banco:
    empresa.to_sql(name='empresa', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_empresa[e] + ' inserido com sucesso no banco de dados!')

try:
    del empresa
except:
    pass
print('Arquivos de empresa finalizados!')
empresa_insert_end = time.time()
empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))
