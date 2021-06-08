import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def execute_step(args):
    description = f'Step: {args.name} - {args.description}'
    with SparkSession.builder.appName(description).getOrCreate() as spark:

        df = spark.read.parquet(args.input_data)


        query.write.mode('overwrite').json(args.output_uri)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto_generate_output')
    parser.add_argument('--output_uri')
    parser.add_argument('--format_output')
    parser.add_argument('--input_dependency_from_output_step')
    parser.add_argument('--from_step')
    parser.add_argument('--input_data')
    parser.add_argument('--name')
    parser.add_argument('--description')
    args = parser.parse_args()
    execute_step(args)