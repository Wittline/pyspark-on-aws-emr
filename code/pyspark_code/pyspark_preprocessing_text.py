import argparse
from pyspark.sql import SparkSession
import logging
from nltk.corpus import stopwords

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def execute_step(description, input, output):

    with SparkSession.builder.appName(description).getOrCreate() as spark:
        
        df = spark.read.parquet(input)
        query.df.filter

                
        query.write.mode('overwrite').json(output)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto_generate_output', type=int)
    parser.add_argument('--output_uri')
    parser.add_argument('--format_output')
    parser.add_argument('--input_dependency_from_output_step',type=int)
    parser.add_argument('--from_step')
    parser.add_argument('--input_data')
    parser.add_argument('--name')
    parser.add_argument('--description')
    parser.add_argument('--prefix_name') 
    args = parser.parse_args()

    description = f'Step: {args.name} - {args.description}'
    output = f's3://{args.prefix_name}/output/{args.output_uri}'

    if args.input_dependency_from_output_step > 0:
        input = f's3://{args.prefix_name}/output/{args.input_data}'
    else:
        input = f's3://{args.prefix_name}/input/{args.input_data}'

    execute_step(description, input, output)