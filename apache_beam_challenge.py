# -*- coding: utf-8 -*-
"""apache_beam_challenge
"""
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems as beam_fs
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
import codecs
import csv
from datetime import datetime
from typing import Dict, Iterable, List
import json

#Lê o dataset

## Cria uma funcao para ler o dataset com schema. Isso é necessário para efetuar o merge depois.
#Herda da classe ptransform para que a função seja interpretada como um "Transform" 
#Herda do with_input_types PBegin pois a função será a primeira a ser executada
#Herda do with_output_types para definir o tipo de retorno da função
@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(Dict[str, str])
def read_csv_lines(pbegin: beam.pvalue.PBegin, filename:str, delimiter=';'):
    """
    Funcao que lê o arquivo csv considerando que a primeira linha contem um header. Para cada linha,
    retorna um dicionario contendo as chaves especificadas no header e seus respectivos valores
    """
    with beam_fs.open(filename) as f:
          # Beam reads files as bytes, but csv expects strings,
          # so we need to decode the bytes into utf-8 strings.
        for row in csv.DictReader(codecs.iterdecode(f, 'utf-8'),delimiter=delimiter):
            yield dict(row)
            
def estado_not_null(element):
    """Funcao que retorna True quando o campo estado não é vazio"""
    return element['estado'] != ''

def filter_columns(element,columns):
    """
    Funcao que dado um dicionario (element) retorna um novo dicionario contendo apenas as chaves listadas em 
    columns
    """
    new_dict = {}
    for k in columns:
    	if k in element.keys():
    		new_dict[k] = element[k]
    	else:
    		new_dict[k] = ''
    yield new_dict
    
def get_max_obitos(element):
    """
    Funcao que dada uma lista de dicionarios (element) retorna aquele que possui o maior (mais atualizado) valor
    para a quantidade de obitos.
    """
    data = element[1]
    mostRecentData = {}
    mostRecentData['obitosAcumulado'] = 0
    for dia in data:
        if int(dia['obitosAcumulado']) > int(mostRecentData['obitosAcumulado']):
            mostRecentData = dia
    yield (mostRecentData['coduf'],mostRecentData)

def get_max_date(element):
    """
    Funcao que dada uma lista de dicionarios (element) retorna aquele que possui o maior (mais atualizado) valor
    para a quantidade de obitos.
    """
    data = element[1]
    mostRecentData = {}
    mostRecentData['data'] = '01/01/2000'
    for dia in data:
        if 'data' in dia.keys():
            if datetime.strptime(dia['data'],format="%d/%m/%Y") > datetime.strptime(mostRecentData['data'],format="%d/%m/%Y"):
                mostRecentData = dia
        else:
            if int(dia['obitosAcumulado']) > int(mostRecentData['obitosAcumulado']):
                mostRecentData = dia
    yield (mostRecentData['coduf'],mostRecentData)

def make_output(element):
    """
    Funcao que recebe o resultado do CoGroupBy.
    Faz o merge dos dois dicionarios, retornando um dicionario contendo o conteudo de ambos
    """
    new_dict = {}
    data_hist = element[1]['hist'][0]
    data_estados = element[1]['estados'][0][0]
    for k in data_hist.keys():
        new_dict[k] = data_hist[k]
    for k in data_estados.keys():
        new_dict[k] = data_estados[k]
    yield new_dict
    
def drop_columns(element,columns):
    """
    Funcao que exclui todas as chaves listadas em columns do dicionario element
    """
    for column in columns:
          element.pop(column,None)
    yield element
    
def rename_columns(element,mapeamento):
    """
    Funcao que dado um dicionario com os dados (element) e um outro dicionario na forma
    "chave":"nova_chave", substitui "chave" por "nova_chave" em todas as ocorrências de element
    """
    for key,value in mapeamento.items():
        element[value] = element[key]
        element.pop(key,None)
    yield element


estados = None
hist = None
result = None
columns_to_drop = ['Código [-]','coduf']
mapeamento = {'Governador [2019]':'Governador','UF [-]':'UF',
              'obitosAcumulado':'TotalObitos','casosAcumulado':'TotalCasos',
              'regiao':'Regiao','estado':'Estado'}

# Especifica em qual plataforma a execucao será feita e passa os argumentos específicos daquela plataforma escolhida
beam_options = PipelineOptions(flags=[], type_check_additional='all')
#Cria o grafo de execução 
with beam.Pipeline(options=beam_options) as pipeline:
    estados = pipeline \
    | 'Read CSV Estados' >> read_csv_lines('EstadosIBGE.csv') \
    | 'Filter columns' >> beam.FlatMap(filter_columns,('Governador [2019]','UF [-]','Código [-]'))\
    | 'Group items' >> beam.GroupBy(lambda s: s['Código [-]'])

	#'Amostra_HIST - Página1.csv'
    hist = pipeline\
    | 'Read CSV HIST' >> read_csv_lines('HIST_PAINEL_COVIDBR_28set2020.csv',delimiter=';') \
    | 'Filtra os estados nulos' >> beam.Filter(estado_not_null)\
    | 'Filter columns' >> beam.FlatMap(filter_columns,('regiao','estado','obitosAcumulado','casosAcumulado','coduf'))\
    | 'Group items' >> beam.GroupBy(lambda s: s['coduf'])\
    | 'Get most recent data' >> beam.FlatMap(get_max_obitos)
    
    result = ({'estados':estados,'hist':hist})\
                    |beam.CoGroupByKey()\
                    |beam.FlatMap(make_output)\
                    |beam.FlatMap(drop_columns,columns_to_drop)\
                    |beam.FlatMap(rename_columns,mapeamento)
                    
keys = result[0].keys()
with open('resultado.csv', 'w', newline='')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(result)

with open('resultado.json', 'w') as fout:
    json.dump(result, fout)
