# MSD Spark

Data processing Engine build on Apache Spark.

## System Dependencies

  * [Python](https://www.python.org/)
  * [Apache Spark](http://spark.apache.org/)
  * [Redis](http://redis.io/)

## Clone the repository

```
git clone https://github.com/prakashthiru/msd_spark.git
```

## Install Packages
```
pip install -r requirements.txt
```

## UNZIP the Data dump
```
unzip data/dump.zip -d data
```

## Copy sample file and edit file as appropriate
```
cp config/database.yaml.sample config/database.yaml
cp config/app.yaml.sample config/app.yaml
```

## Run
```
/bin/spark-submit analytics.py
```