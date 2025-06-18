def readCsvAsDataframe(spark_session, path):
    return spark_session.read.option("header", True).option("inferSchema", True).csv(path)
