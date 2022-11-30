import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType
from typer import Option, Typer

logger = logging.getLogger(__name__)
app = Typer()


def validate_ip_address(col: str) -> bool:
    """

    Args:
        col:  Name of the column to validate

    Returns: Boolean value

    """
    values = col.split(".")
    for element in values:
        if int(element) < 0 or int(element) > 255 or len(values) != 4:
            return False

    return True


def filter_ip(df: DataFrame) -> DataFrame:
    """
    Filters valid ip address

    Args:
        df: Input spark data frame

    Returns: Input spark data frame

    """
    convertUDF = F.udf(lambda z: validate_ip_address(z), BooleanType())
    df = df.withColumn("is_valid_ip", convertUDF(F.col("ip_address")))
    df = df.filter(df.is_valid_ip == True)
    return df


def cast_country_to_capital(df: DataFrame, col: str) -> DataFrame:
    """
    Converts all countries first letter to capital
    Args:
        df: Input spark dataframe
        col: Column name to capitalize

    Returns: Spark DataFrame

    """
    df = df.withColumn(col, F.initcap(F.col(col)))
    return df


@app.command()
def clean_data(
    input_path: str = Option(
        ...,
        help="Input path of the raw  file",
    ),
    output_path: str = Option(
        ...,
        help="Output path of the cleaned json file",
    ),
):
    """
    Reads raw file performs clean steps write the output to a clean file
    Args:
        input_path: Input path of file to clean
        output_path: Output to write the cleaned file

    Returns: None

    """
    spark = SparkSession.builder.master("local[1]").appName("Clean Data").getOrCreate()
    logger.info(f"Reading raw file from {input_path}")
    df = spark.read.json(input_path, multiLine=True)

    logger.info(f"Changing countries to capital letters")

    df = cast_country_to_capital(df, "country")

    logger.info(f"Filtering ip address")
    df = filter_ip(df)

    df.createOrReplaceTempView("cleaned_data")

    logger.info(f"Writing cleaned filed to {output_path}/cleaned.json")

    df.write.mode("overwrite").json(f"{output_path}/cleaned.json")


@app.command()
def analyze_data(
    input_path: str = Option(
        ...,
        help="Input path of cleand json file",
    ),
    output_path: str = Option(..., help="Output path of cleaned analayzed json file"),
):
    """
    Reads clean file performs analysis steps and writes the output to a  file
    Args:
        input_path: Input path of file to analyze
        output_path: Output to write the cleaned file

    Returns: None

    """

    spark = (
        SparkSession.builder.master("local[1]").appName("Analyze Data").getOrCreate()
    )
    logger.info(f"Reading cleaned file from {input_path}")
    df = spark.read.json(input_path)
    df.createOrReplaceTempView("cleaned_data")

    df_max_countries = spark.sql(
        "select country,count(*) as count from cleaned_data group by country order by\
                                     count desc LIMIT 1"
    )

    df_min_countries = spark.sql(
        "select country,count(*) as count from cleaned_data group by country order by\
                                     count,country LIMIT 1"
    )

    df_distinct_users = spark.sql(
        "select count(distinct(email)) as distinct_users from cleaned_data"
    )

    max_country = df_max_countries.collect()
    distinct_users = df_distinct_users.collect()
    min_country = df_min_countries.collect()

    result = {}
    for row in max_country:
        result["most_frequent_country"] = [row["country"], row["count"]]

    for row in min_country:
        result["min_frequent_country"] = [row["country"], row["count"]]

    for row in distinct_users:
        result["distinct_users"] = row["distinct_users"]

    import json

    logger.info(f"Writing analyzed file to {output_path}/analyzed.json")

    with open(f"{output_path}/analyzed.json", "w") as fp:
        json.dump(result, fp, indent=4)


@app.command()
def run_local_pipeline(
    input_path: str = Option(
        ...,
        help="Input path of the raw  file",
    ),
    output_path: str = Option(
        ...,
        help="Output path of the analyzed json file",
    ),
):
    """
    Run the pipeline locally without airflow.

    Args:
        input_path: Input path of the raw file
        output_path: Output path of the analyzed file.

    Returns:

    """
    logger.info("Cleaning data from raw data...")
    clean_data(input_path=input_path, output_path=output_path)
    logger.info("Completed data cleaning..")
    logger.info("Calling analyze data...")
    analyze_data(input_path=f"{output_path}/cleaned.json", output_path=output_path)
    logger.info("Completed analysis of data")


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    app()
