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

#print('Descompactando arquivo:')
#with zipfile.ZipFile(OUTPUT_FILES_PATH) as zip_ref:
 #   zip_ref.extractall(extracted_files)
    
#%%
#Arquivo socios:
socios_insert_start = time.time()
print("Gerando arquivos de SÓCIOS para o banco de dados:")

#Dropping tabela antes de inserí-la
cur.execute('DROP TABLE IF EXISTS "socios";')
conn.commit()

for e in range(0, len(arquivos_socios)):
    print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
    try:
        del socios
    except:
        pass

    extracted_file_path = Path(f'{extracted_files}/{arquivos_socios[e]}')
    socios = pd.DataFrame(columns=[1,2,3,4,5,6,7,8,9,10,11])
    socios = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          engine='python',
                          encoding='cp1252',
                          #nrows=100,
                          skiprows=0,
                          header=None,
                          dtype='object')

    #Tratamento do arquivo antes de inserir na base:

    #Excluindo coluna index
    socios = socios.reset_index()
    del socios['index']

    #Renomeando colunas conforme o layout
    socios.columns = ['cnpj_basico',
                      'identificador_socio',
                      'nome_socio_razao_social',
                      'cpf_cnpj_socio',
                      'qualificacao_socio',
                      'data_entrada_sociedade',
                      'pais',
                      'representante_legal',
                      'nome_do_representante',
                      'qualificacao_representante_legal',
                      'faixa_etaria']

    #Gravação de tabela transformada no dados no banco:
    socios.to_sql(name='socios', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_socios[e] + ' inserido com sucesso no banco de dados!')

try:
    del socios
except:
    pass
print('Arquivos de socios finalizados!')
socios_insert_end = time.time()
socios_Tempo_insert = round((socios_insert_end - socios_insert_start))
print('Tempo de execução do processo de sócios (em segundos): ' + str(socios_Tempo_insert))
