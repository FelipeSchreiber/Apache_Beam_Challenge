# Apache_Beam_Challenge
Repositório destinado a criar um pipeline utilizando Apache Beam
## Table of Contents
<!--ts-->
- [About](#about)
- [Pipeline](#pipeline)
- [Requirements](#requirements)
- [How to use](#how-to-use)
- [Technologies](#technologies)
<!--te-->

## About
The challenge of the present work was to build a pipeline using Apache Beam framework. The main reason to do so was to create a pipeline decoupled from the runner engine, which can be Apache Flink, Apache Spark, DataFlow and others. Hence, given the files EstadosIBGE.csv and HIST_PAINEL_COVIDBR, we wish to obtain a result file containing an aggregation of data per state, in the format:

```json
{
    'Regiao':xxxx,
    'Estado':xxxx,
    'UF':xxxx,
    'Governador':xxxx,
    'TotalCasos':xxxx,
    'TotalObitos':xxxx
}    
```

## Pipeline

In order to build the pipeline, the following Apache Beam built-in functions/methods were used:
- PipelineOptions: Define which runner engine will be used, as well as their parameters. The DataflowRunner was used, but any other could be specified.

- Pipeline: Build the pipeline execution graph

- beam.io.FileSystems: To read the csv

- FlatMap: This function allows us to apply a custom function to each row of the Pcollection.

- Filter: To filter the desired columns of the data

- GroupBy: To group the data according to a specified key

- CoGroupByKey(): To merge two Pcollections that share identical key (in SQL notation, this resembles a join operation)

Once the functions were specified, let's look how the pipeline looks like:
<p align="center">
    <img src="https://github.com/FelipeSchreiber/Apache_Beam_Challenge/blob/main/HURB_BEAM.drawio.png?raw=true"/>
</p>

In order to produce the above pipeline, the following functions were created (more description in code):
- read\_csv\_lines: Inherits from ptransform and typehints.with_input_types. This function reads the csv and generate a dictionary for each row, containing the keys specified in header and values from the current line. This function is used in ingest step.

- estado_not_null: returns true when "estado" key is not null. Used in filter step.

- filter_columns: given the pcollection and the keys to keep up, this function returns a subdict containing the desired "columns"(keys). Used in filter step.

- get_max_obitos: Given an array of dictionaries, returns the one that contains the largest (most recent data of) "obitosAcumulados". An approch returning the latest "date" could also be taken, but both seems to be equivalent. Used in "get obitos acumulado per coduf" step.

- make_output: Given the result of coGroupBy(), this function merges both dictionaries under the same aggregated key. Used in "make output" step.

- drop_columns: given a list of columns to drop, remove them from all dictionaries in a Pcollection. Used in drop step.

- rename_columns: given a dict containing "key":"new_key", replaces all "key" in a pcollection for "new_key". Used in rename step.