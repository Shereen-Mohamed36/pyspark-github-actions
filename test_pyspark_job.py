from pyspark.sql import SparkSession
import pytest
from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
  spark_session = (
      SparkSession.builder.appName("PySpark-CI-Test")
      .master("local[2]")
      .getOrCreate()
  )
  yield spark_session
  spark_session.stop()


def test_clean_data(spark):
  data = [
      (1, "noura", 100.0),
      (2, "salma", 0.0),
      (3, "ahmed", -50.0),
      (4, None, 200.0),
      (5, "Diana", 50.0),
  ]

  schema = ["id", "name", "amount"]
  df = spark.createDataFrame(data, schema)

  result_df = clean_data(df)
  result_data = result_df.collect()

  assert len(result_data) == 2

  alice_row = result_df.filter(result_df.id == 1).collect()[0]
  assert alice_row["amount_with_tax"] == pytest.approx(120.0)

  diana_row = result_df.filter(result_df.id == 5).collect()[0]
  assert diana_row["amount_with_tax"] == pytest.approx(60.0)
