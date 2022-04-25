### Using Dataproc for running batch jobs

- To download Dataset: 
```
!wget https://archive.ics.uci.edu/ml/machine-learning-databases/kddcup99-mld/kddcup.data_10_percent.gz
```

- To create a GCS bucket:

```cmd
BUCKET='cloud-training-demos-ml2031'
!gsutil mb gs://$BUCKET
```

- To Copy data into Bucket:
```cmd
!gsutil cp kdd* gs://$BUCKET/
```

- Reading Data from Bucket using Spark
```python
from pyspark.sql import SparkSession, SQLContext, Row

spark = SparkSession.builder.appName("kdd").getOrCreate()
sc = spark.sparkContext
data_file = "gs://{}/kddcup.data_10_percent.gz".format(BUCKET)
raw_rdd = sc.textFile(data_file).cache()
```

- Formatting data in the form:
```python
csv_rdd = raw_rdd.map(lambda row: row.split(","))
parsed_rdd = csv_rdd.map(lambda r: Row(
    duration=int(r[0]), 
    protocol_type=r[1],
    service=r[2],
    flag=r[3],
    src_bytes=int(r[4]),
    dst_bytes=int(r[5]),
    wrong_fragment=int(r[7]),
    urgent=int(r[8]),
    hot=int(r[9]),
    num_failed_logins=int(r[10]),
    num_compromised=int(r[12]),
    su_attempted=r[14],
    num_root=int(r[15]),
    num_file_creations=int(r[16]),
    label=r[-1]
    )
)
parsed_rdd.take(5)
```
- Converting into dataframes
```python
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(parsed_rdd)
```
- To write data back to GCS bucket

```python
df.write.format("csv").mode("overwrite").save("gs://{}/output/".format(BUCKET))
```

- To create a bash file for script to kick off
```
nano submit_onejob.sh

chmod +x submit_onejob.sh

./submit_onejob.sh $PROJECT_ID
```
