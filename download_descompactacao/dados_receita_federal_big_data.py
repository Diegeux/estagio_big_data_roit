from datetime import date
import dotenv
from pathlib import Path
from sqlalchemy import create_engine
import bs4 as bs
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

dados_rf = 'http://200.152.38.155/CNPJ/'
output_files = Path(getEnv('OUTPUT_FILES_PATH'))
extracted_files = Path(getEnv('EXTRACTED_FILES_PATH'))
raw_html = urllib.request.urlopen(dados_rf)
raw_html = raw_html.read()

#Formatação da página web, conversão do conteúdo em String
page_items = bs.BeautifulSoup(raw_html, 'lxml') #extração de dados html e xml
html_str = str(page_items)

#Obtenção de arquivo .zip
Files = []
text = '.zip'

for m in re.finditer(text, html_str):
    i_start = m.start()-40
    i_end = m.end()
    i_loc = html_str[i_start:i_end].find('href=')+6
    print(html_str[i_start+i_loc:i_end])
    Files.append(html_str[i_start+i_loc:i_end])

del(Files[0:7])
del(Files[3::])

print('Os seguintes arquivos serão baixados:')

i_f=0
for f in Files:
    i_f += 1
    print(str(i_f) + ' - ' + f)

#Downloading dos arquivos
def bar_progress(current, total, width=80):
  progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
  #Não usar o print()
  sys.stdout.write("\r" + progress_message)
  sys.stdout.flush()

i_l = 1
for l in Files:
    i_l += 0
    print('Baixando arquivo:')
    print(str(i_l) + ' - ' + l)
    url = dados_rf+l
    wget.download(url, out=str(output_files), bar=bar_progress)

Layout = 'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf'
print('Baixando layout:')
wget.download(Layout, out=str(output_files), bar=bar_progress)

# Diretório para extração dos arquivos:
if not os.path.exists(extracted_files):
    os.mkdir(extracted_files)

# Extracting files:
i_l = 0
for l in Files:
    try:
        i_l += 1
        print('Descompactando arquivo:')
        print(str(i_l) + ' - ' + l)
        with zipfile.ZipFile(output_files / l, 'r') as zip_ref:
            zip_ref.extractall(extracted_files)
    except:
        pass

insert_start = time.time()
extracted_files = Path(getEnv('EXTRACTED_FILES_PATH'))
#Arquivos
Items = [name for name in os.listdir(extracted_files) if name.endswith('')]

#Separação de arquivos
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

#Configuração e credenciais de conexão com o banco de dados - PostgreSQL
user=getEnv('DB_USER')
passw=getEnv('DB_PASSWORD')
host=getEnv('DB_HOST')
port=getEnv('DB_PORT')
database=getEnv('DB_NAME')

#Conexão
engine = create_engine('postgresql://'+user+':'+passw+'@'+host+':'+port+'/'+database)
conn = psycopg2.connect('dbname='+database+' '+'user='+user+' '+'host='+host+' '+'password='+passw)
cur = conn.cursor()

#Arquivo empresa:
empresa_insert_start = time.time()
print("Gerando arquivos de EMPRESA para o banco de dados:")

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

    #Gravação de tabela backup no banco de dados
    empresa.to_sql(name='empresa_backup', con=engine, if_exists='append', index=False)

    #Tratamento do arquivo antes de inserir na base:
    empresa = empresa.reset_index()
    del empresa['index']

    #Renomear colunas conforme o layout
    empresa.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

    #Substituição de valores conforme o layout
    empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',','.'))
    empresa['capital_social'] = empresa['capital_social'].astype(float)

    #Substituição de valores conforme o layout
    empresa['porte_empresa'] = empresa['porte_empresa'].apply(lambda x: x.replace('02','MICRO EMPRESA'))
    empresa['porte_empresa'] = empresa['porte_empresa'].apply(lambda x: x.replace('03','EMPRESA DE PEQUENO PORTE'))
    empresa['porte_empresa'] = empresa['porte_empresa'].apply(lambda x: x.replace('05','DEMAIS'))
    
      

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
    
    #Gravação de tabela backup no banco de dados
    estabelecimento.to_sql(name='estabelecimento_backup', con=engine, if_exists='append', index=False)

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

    #Substituição de valores conforme o layout
    estabelecimento['identificador_matriz_filial']=estabelecimento['identificador_matriz_filial'].apply(lambda x: x.replace('1','MATRIZ'))
    estabelecimento['identificador_matriz_filial']=estabelecimento['identificador_matriz_filial'].apply(lambda x:x.replace('2','FILIAL'))
    
    
    #Substituição de valores conforme o layout
    estabelecimento['situacao_cadastral']=estabelecimento['situacao_cadastral'].apply(lambda x:x.replace('01','NULA'))
    estabelecimento['situacao_cadastral']=estabelecimento['situacao_cadastral'].apply(lambda x:x.replace('02','ATIVA'))
    estabelecimento['situacao_cadastral']=estabelecimento['situacao_cadastral'].apply(lambda x:x.replace('03','SUSPENSA'))
    estabelecimento['situacao_cadastral']=estabelecimento['situacao_cadastral'].apply(lambda x:x.replace('04','INAPTA'))
    estabelecimento['situacao_cadastral']=estabelecimento['situacao_cadastral'].apply(lambda x:x.replace('08','BAIXADA'))

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

    #Gravação de tabela backup no banco de dados
    socios.to_sql(name='socios_backup', con=engine, if_exists='append', index=False)

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

    #Substituição de valores conforme o layout
    socios['identificador_socio']=socios['identificador_socio'].apply(lambda x:x.replace('1','PESSOA JURIDICA'))
    socios['identificador_socio']=socios['identificador_socio'].apply(lambda x:x.replace('2','PESSOA FISICA'))
    socios['identificador_socio']=socios['identificador_socio'].apply(lambda x:x.replace('3','ESTRANGEIRO'))

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


