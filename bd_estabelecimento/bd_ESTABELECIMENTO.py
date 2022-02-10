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
    

#Arquivo estabelecimento:
estabelecimento_insert_start = time.time()
print("Gerando arquivos de ESTABELECIMENTO para o banco de dados:")

#Dropping tabela antes de inserí-la
cur.execute('DROP TABLE IF EXISTS "estabelecimento";')
conn.commit()

for e in range(0, len(arquivos_estabelecimento)):
    print('Trabalhando no arquivo: '+arquivos_estabelecimento[e]+' [...]')
    try:
        del estabelecimento
    except:
        pass

    estabelecimento = pd.DataFrame(columns=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28])
    extracted_file_path = Path(f'{extracted_files}/{arquivos_estabelecimento[e]}')

    estabelecimento = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          engine='python',
                          encoding= 'cp1252',
                          #nrows=100,
                          skiprows=0,
                          header=None,
                          dtype='object')
    
    #Tratamento do arquivo antes de inserir na base:

    #Excluindo coluna index
    estabelecimento = estabelecimento.reset_index()
    del estabelecimento['index']

    #Renomeando colunas conforme o layout
    estabelecimento.columns = ['cnpj_basico',
                               'cnpj_ordem',
                               'cnpj_dv',
                               'identificador_matriz_filial',
                               'nome_fantasia',
                               'situacao_cadastral',
                               'data_situacao_cadastral',
                               'motivo_situacao_cadastral',
                               'nome_cidade_exterior',
                               'pais',
                               'data_inicio_atividade',
                               'cnae_fiscal_principal',
                               'cnae_fiscal_secundaria',
                               'tipo_logradouro',
                               'logradouro',
                               'numero',
                               'complemento',
                               'bairro',
                               'cep',
                               'uf',
                               'municipio',
                               'ddd_1',
                               'telefone_1',
                               'ddd_2',
                               'telefone_2',
                               'ddd_fax',
                               'fax',
                               'correio_eletronico',
                               'situacao_especial',
                               'data_situacao_especial']

   
    #Gravação de tabela transformada no dados no banco:
    estabelecimento.to_sql(name='estabelecimento', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_estabelecimento[e] + ' inserido com sucesso no banco de dados!')

try:
    del estabelecimento
except:
    pass
print('Arquivos de estabelecimento finalizados!')
estabelecimento_insert_end = time.time()
estabelecimento_Tempo_insert = round((estabelecimento_insert_end - estabelecimento_insert_start))
print('Tempo de execução do processo de estabelecimento (em segundos): ' + str(estabelecimento_Tempo_insert))
