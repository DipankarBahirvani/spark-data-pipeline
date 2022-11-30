from pyspark.sql.types import StringType


def test_validate_ip_address():
    from processing import validate_ip_address

    assert validate_ip_address("255.0.0") == False
    assert validate_ip_address("192.0.1.244") == True
    assert validate_ip_address("280.256.244.255") == False


def test_filter_valid_ip(spark):
    data = [
        "127.0.0.1",
        "125.0.0.1",
        "257.255.0.1",
    ]
    df = spark.createDataFrame(data, StringType()).toDF("ip_address")
    from processing import filter_ip

    df = filter_ip(df)

    assert df.count() == 2


def test_capitalize_country(spark):
    data = [
        "india",
        "germany",
    ]
    df = spark.createDataFrame(data, StringType()).toDF("country")
    from processing import cast_country_to_capital

    df = cast_country_to_capital(df, col="country")
    countries = df.collect()

    result = []
    for country in countries:
        result.append(country["country"])

    assert ("India" in result) == True
    assert ("Germany" in result) == True
