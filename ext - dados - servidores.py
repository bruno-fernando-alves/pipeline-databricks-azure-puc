# Databricks notebook source
!pip install -q findspark
import os
import requests as rs
import findspark
import gzip

# COMMAND ----------

# Caminho da pasta que você deseja verificar/criar
pasta_alvo = '/content/inbound'
# Verificar se a pasta já existe
if not os.path.exists(pasta_alvo):
    # Criar a pasta se ela não existir
    os.makedirs(pasta_alvo)
    print(f'Pasta "{pasta_alvo}" criada com sucesso!')
else:
    print(f'A pasta "{pasta_alvo}" já existe.')

# COMMAND ----------


anos_meses = [f"{ano}-{str(mes).zfill(2)}" for ano in range(2012, 2023) for mes in range(1, 13)]
for ano_m in anos_meses:
  files = rs.get(f'https://dados.mg.gov.br/dataset/98b58ea9-813e-4f50-8555-4ec0e15bbe91/resource/11c7a7a0-c50f-4a2d-b309-1547e94e6fe8/download/servidores-{ano_m}.csv.gz')

  if files.status_code == 200:

      with open(f"/content/inbound/servidores_{ano_m}.csv",'wb') as arquivo:
        arquivo.write(gzip.decompress(files.content))
      print(f'sucesso ao salvar o arquivo servidores_{ano_m}')
  else:
    print('erro ao salvar')



# COMMAND ----------

findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Silver - servidores ") \
    .getOrCreate()
