from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session():
    """
    creates spark session.
    :return: spark session.
    """
    # todo: add hadoop config.
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_deaths_data(spark, input_path, output_path):
    """
    read deaths data from source, extract required fields,
    partition it and store.
    :param spark: spark session
    :param input_path: input path of deaths data of all the acountries.
    :param output_path: location to store partitioned data.
    :return: none.
    """
    deaths_data = f'{input_path}/deaths-counts-hmd.csv'
    deaths_df = spark.read.csv(deaths_data, header=True, sep=';').dropDuplicates()

    deaths_df = deaths_df.select(
        col('Year').alias('year'),
        col('Male').alias('male'),
        col('Female').alias('female'),
        col('Total').alias('total'),
        col('Country Code').alias('country_code'),
        col('Country').alias('country')
    )

    dm_deaths_df = deaths_df \
        .filter(col('year').isin([2010, 2011, 2012, 2013])) \
        .select('male', 'female', 'total', 'year', 'country_code')

    dm_deaths_df.write.format('parquet') \
        .partitionBy(['country_code', 'year']) \
        .mode('overwrite') \
        .save(f'{output_path}/dim_deaths')


def process_population_data(spark, input_path, output_path):
    """
    read population data from source, extract required fields,
    partition it and store.
    :param spark: spark session
    :param input_path: input path of population data of all the countries.
    :param output_path: location to store partitioned data.
    :return: none.
    """
    population_data = f'{input_path}/population-hmd.json'
    df = spark.read.json(population_data).dropDuplicates()

    population_df = df \
        .withColumn('male', col('fields.male')) \
        .withColumn('female', col('fields.female')) \
        .withColumn('total', col('fields.total')) \
        .withColumn('year', col('fields.year')) \
        .withColumn('country', col('fields.country')) \
        .withColumn('country_code', col('fields.country_code')) \
        .select('male', 'female', 'year', 'total', 'country', 'country_code')

    dim_country_df = population_df.select(col('country').alias('name'), col('country_code')).dropDuplicates()
    dim_country_df.write.format('parquet').mode('overwrite').save(f'{output_path}/dim_country')

    dm_population_df = population_df \
        .filter(col('year').isin([2010, 2011, 2012, 2013])) \
        .select('male', 'female', 'total', 'year', 'country_code')

    dm_population_df.write.format('parquet') \
        .partitionBy(['country_code', 'year']) \
        .mode('overwrite') \
        .save(f'{output_path}/dim_population')


def get_population_vs_deaths_ratio(spark, output_path):
    """
    generate final data. i.e compare and combine data of
    population and deaths of a country in a specified year.
    :param spark: spark session.
    :param output_path: location to store the final data.
    :return: none.
    """
    deaths_data = spark.read.format('parquet'). \
        load(f'{output_path}/dim_deaths/') \
        .repartition('country_code', 'year')

    population_data = spark.read.format('parquet') \
        .load(f'{output_path}/dim_population/') \
        .repartition('country_code', 'year')

    country_data = spark.read.format('parquet').load(f'{output_path}/dim_country/')

    deaths_data.createOrReplaceTempView('death')
    population_data.createOrReplaceTempView('population')

    population_vs_deaths_df = population_data.join(deaths_data,
                                                   [
                                                       population_data.country_code == deaths_data.country_code,
                                                       population_data.year == deaths_data.year
                                                   ],
                                                   'left') \
        .select(
        population_data.male.alias('male_population'),
        population_data.female.alias('female_population'),
        population_data.total.alias('total_population'),
        deaths_data.male.alias('male_deaths'),
        deaths_data.female.alias('female_deaths'),
        deaths_data.total.alias('total_deaths'),
        population_data.country_code,
        population_data.year
    )

    population_vs_deaths_df = population_vs_deaths_df \
        .groupBy('country_code', 'year') \
        .agg(
        {
            'male_population': 'sum',
            'female_population': 'sum',
            'total_population': 'sum',
            'male_deaths': 'sum',
            'female_deaths': 'sum',
            'total_deaths': 'sum',
        }
    ) \
        .withColumnRenamed('Sum(male_population)', 'male_population') \
        .withColumnRenamed('Sum(female_deaths)', 'female_deaths') \
        .withColumnRenamed('Sum(total_population)', 'total_population') \
        .withColumnRenamed('Sum(female_population)', 'female_population') \
        .withColumnRenamed('Sum(male_deaths)', 'male_deaths') \
        .withColumnRenamed('Sum(total_deaths)', 'total_deaths')

    population_vs_deaths_df.show(truncate=False)
    country_data.show(truncate=False)

    population_vs_deaths_df = population_vs_deaths_df \
        .join(country_data, [population_vs_deaths_df.country_code == country_data.country_code]) \
        .select(
        population_vs_deaths_df.year,
        population_vs_deaths_df.total_population,
        population_vs_deaths_df.country_code,
        population_vs_deaths_df.total_deaths,
        population_vs_deaths_df.female_population,
        population_vs_deaths_df.male_deaths,
        population_vs_deaths_df.male_population,
        population_vs_deaths_df.female_deaths,
        country_data.name
    )

    population_vs_deaths_df.show(truncate=False)

    population_vs_deaths_df.write \
        .format('parquet') \
        .partitionBy(['country_code', 'year']) \
        .mode('overwrite') \
        .save(f'{output_path}/population_deaths_ratio/')


def quality_checks(spark_session, data_path):
    """
    check if data exists and data of required year is not null.
    :param spark_session: spark session.
    :param data_path: path to read data from.
    :return:
    """
    population_vs_deaths_df = spark_session.read.parquet(data_path)
    if population_vs_deaths_df.rdd.isEmpty():
        raise Exception('No Data in destination path.')

    if not population_vs_deaths_df.filter(col('year').isin([2010, 2011, 2012, 2013])):
        raise Exception('No Data in destination path.')

def main():
    """
    main method to run pipeline.
    """
    spark = create_spark_session()
    input_path = 'data/'
    output_path = 'output/'

    process_deaths_data(spark, input_path, output_path)
    process_population_data(spark, input_path, output_path)

    get_population_vs_deaths_ratio(spark, output_path)

    quality_checks(spark, f'{output_path}/population_deaths_ratio/')


if __name__ == "__main__":
    main()
