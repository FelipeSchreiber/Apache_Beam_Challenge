# Apache_Beam_Challenge
Repositório destinado a criar um pipeline utilizando Apache Beam
## Table of Contents
<!--ts-->
- [About](#about)
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
