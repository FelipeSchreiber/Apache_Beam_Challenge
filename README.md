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

##Pipeline

In order to build the pipeline, the following Apache Beam built-in functions/methods were used:
- PipelineOptions: Define which runner engine will be used, as well as their parameters. The DataflowRunner was used, but any other could be specified.

- Pipeline: Build the pipeline execution graph

- beam.io.FileSystems: To read the csv

- FlatMap: This function allows us to apply a custom function to each row of the Pcollection.

- Filter: To filter the desired columns of the data

- GroupBy: To group the data according to a specified key

- CoGroupByKey(): To merge two Pcollections that share identical key (in SQL notation, this resembles a join operation)

Once the functions were specified, let's look how the pipeline looks like:
![alt text](https://github.com/FelipeSchreiber/Apache_Beam_Challenge/blob/main/HURB_BEAM.drawio.png?raw=true)

Also, the following functions were created, in order to provide the desired output