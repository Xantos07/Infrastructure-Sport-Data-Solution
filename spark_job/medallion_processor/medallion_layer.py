from pyspark.sql import SparkSession

class MedallionLayer:
    def __init__(self, spark_session, settings):
        self.spark = spark_session
        self.settings = settings

    def create_spark_session(self, appname, spark_settings):
        """Crée une session Spark avec support Delta Lake + MinIO"""

        return SparkSession.builder \
            .appName(appname) \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", spark_settings.minio_base_url) \
            .config("spark.hadoop.fs.s3a.access.key", spark_settings.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", spark_settings.minio_password) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
    

    def read_delta(self, path):
        return self.spark.read.format("delta").load(path)

    def write_delta(self, df, path, mode="overwrite"):
        df.write.format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .save(path)
            
    def log_step(self, message):
        print(f"\n{'='*20} {message} {'='*20}")