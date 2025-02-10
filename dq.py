import pydeequ
from pydeequ.checks import *
from pydeequ.verification import *
from datasets import Dataset
import pandas as pd

def is_valid():
    spark = (SparkSession
        .builder
        .config("spark.jars.packages", pydeequ.deequ_maven_coord)
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .config("spark.driver.host", "localhost")
        .appName('appname')
        .getOrCreate())

    deduplicated_ds = Dataset.from_file("output/deduplicated/dataset.arrow")
    deduplicated_df = pd.DataFrame(deduplicated_ds)

    # Constraint Verification
    check = Check(spark, CheckLevel.Warning, "")

    checkResult = VerificationSuite(spark) \
        .onData(deduplicated_df) \
        .addCheck(
            check.hasSize(lambda x: x >= 997) \
            .isComplete("content") \
            .isComplete("issues") \
            .hasCompleteness("main_language", assertion=lambda x: x >= 0.8) \
            .isComplete("size") \
            .hasCompleteness("stars", assertion=lambda x: x >= 0.8)) \
        .run()

    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    checkResult_df.show(truncate=False)

    failure_count = checkResult_df.where(checkResult_df.constraint_status != "Success").count()
    # Stop
    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()

    return failure_count == 0


if __name__ == "__main__":
    is_valid()