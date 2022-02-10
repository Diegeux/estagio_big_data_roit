# PIPELINE DE DADOS - RECEITA FEDERAL

![receita](https://user-images.githubusercontent.com/39379425/153439946-7f589aac-83a7-48d6-b7dc-e4dad69d832a.gif)

Essa documentação apresentará o processo de criação de Pipeline de Dados para realizar o processamento dos dados de empresa, estabelecimento e sócio da base pública de
CNPJs da Receita Federal e disponibilizá-los em um banco de dados estruturados para consumo.

A documentação satisfaz os tópicos abaixo:

- Montar um pipeline de dados para processamento da base pública de CNPJs da Receita Federal e disponibilizar em um banco de dados escolhido pelo desenvolvedor.
- Realizar o download automático dos dados diretamente do site da receita;
- Implementar scheduler para execução periódica (crontab, windows task scheduler);
- O fluxo do projeto deve acompanhar as camadas do datalake;
- Implementar esse fluxo na própria máquina;


# Infraestrutura
Sistema Operacional:
- Windows 10

Linguagem de programação:
- Python 3.9.1 

Bibliotecas:
- datetime
- dotenv
- pathlib
- sqlalchemy
- bs4
- os
- pandas
- psycopg2
- re
- sys
- time
- urllib.request
- wget
- zipfile

Banco de Dados:
- PostgreSQL 13.2

Tabelas:
- Para melhor entender os dados, é necessário conhecer os atributos das tabelas e a sua significância. Para isso, consultar nesse repositório o documento NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf

# Desenvolvimento

Pipeline de dados:
- Os dados foram consumidos a partir do site da Receita Federal: http://200.152.38.155/CNPJ/.
![index_cnpj](https://user-images.githubusercontent.com/39379425/153449373-3b63c22c-f0c8-4889-8c5f-7607850b8413.jpg)

- No repositório, há 37 grandes arquivos, porém, devido ao baixo poder de processamento da máquina, foi feito o download de apenas 3 arquivos (um de cada categoria - Empresa, Estabelecimento e Sócio), e com o auxílio do Microsoft Excel e Bloco de notas, esses arquivos foram editados para conter aproximadamente as primeiras 1.000 linhas de registro.
![3_files_download](https://user-images.githubusercontent.com/39379425/153449755-eab416a4-909e-4fe6-847f-941bf19f617b.jpg)

- Cada arquivo foi baixado no formato .zip, logo após, foi descompactado e alocado em um diretório diferente. Posteriormente, foi gerado uma tabela backup para cada arquivo, com os dados "puros", foi gerado as limpezas e transformações necessárias, e por fim, os dados foram encaminhados para o banco de dados para o armazenamento.
![image](https://user-images.githubusercontent.com/39379425/153452127-270a1ac8-e3a0-481b-b270-cab2891d9b94.png)

- O código desenvolvido será executado periodicamente de forma automática a partir do Windows Task Scheduler.

Criação do agendador de tarefa.
![1](https://user-images.githubusercontent.com/39379425/153452817-bbee38ad-dbd3-4c84-bd2b-ca5960615d01.jpg)

Informando o programa de execução e arquivo que será executado pelo programa. O programa citado é o python da virtual environment (ambiente virtual), cujo foi criado, configurado e utilizado no desenvolvimento.
![2](https://user-images.githubusercontent.com/39379425/153452859-12d18a68-19fb-4ae5-8fdc-316c4146120f.jpg)

O arquivo será executado diariamente no intervalo de 1 hora, a partir do dia 09/02/2022 as 14:25h.
![3](https://user-images.githubusercontent.com/39379425/153452864-2335df70-d6cc-4b7f-b9ff-a175bab18994.jpg)

Para desenvolvimento em máquinas de terceiros é aconselhável (Sistema Operacional Windows):

- Criar uma virtual environment (ambiente virtual):

python -m venv "nome_do_ambiente_virtual"

- Iniciar o ambiente virtual:

nome_do_ambiente_virtual/Scripts/Activate

- Instalação do arquivo requeriments.txt (consulte esse repositório) no seu ambiente virtual:

python -m pip install -r requirements.txt

- Verifique as bibliotecas instaladas:

pip list

# Plus - Analytics
Para melhorar nosso desenvolvimento, foi criado um relatório no Microsoft Power BI para conseguir visualizar melhor os dados, para tomadas de decisão mais rápidas e assertivas. 
![image](https://user-images.githubusercontent.com/39379425/153464856-2f1a0083-873d-4f7c-be6f-19cf28f87c49.png)

Na Dashboard podemos responder diversas perguntas facilmente, dentre elas, temos:

- Qual é a quantidade de empresas por porte?
- Qual é a maior e menor faixa etária dos sócios?
- Qual é a quantidade de estabelecimento por identificação e situação?
- Como está distribuído a quantidade de estabelecimento por estado(UF)?

# Conclusão

Toda etapa do Pipeline de dados é extremamente importante, seja ela a parte de engenharia, preparação ou análises dos dados. O processo de coleta tem que ser bem definido, para que depois disso sejam feitas as limpezas e os tratamentos dos dados, logo após, deveremos fazer um bom refinamento e enriquecimento dos dados, no entanto, proporcionando ao nosso ecosistema de dados uma grande capacidade de armazenamento contínuo, contemplando os 5 V's do Big Data (Volume, Velocidade, Variedade, Veracidade e Valor) e reduzindo desperdício e gerando mais riquezas para as corporações.








