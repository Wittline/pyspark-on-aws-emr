import argparse
import logging
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from os import path
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
import os
import boto3
from io import BytesIO
from wordcloud import WordCloud, STOPWORDS


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def create_spark_session(description):

    logger.info("Reading spark object")
    spark = SparkSession\
            .builder\
            .appName(description)\
            .getOrCreate()
    
    logger.info("Spark object ready")
    return spark

def from_s3(s3, bucket, key):        
        file_byte_string = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        return Image.open(BytesIO(file_byte_string))


def to_s3(s3, filename, bucket, key):            
        object_name = key + "/{fname}".format(fname= os.path.basename(filename))
        sent_data = s3.upload_file(filename, bucket, object_name)


def execute_step(spark, input, output, args):

        logger.info("Executing step...")
        df = spark.read.parquet(input)
        us_mask = np.array(from_s3(args.prefix_name,f'input/usa.png'))
        s3 = boto3.client('s3')
        years = list(range(1995, 2016))

        for y in years:
            text = df.where("year == " + str(y)).first()['words']            
            stopwords = set(STOPWORDS)
            wc = WordCloud(background_color="white", max_words=20000, mask=us_mask, stopwords=stopwords)
            wc.generate(text)
            path_file = path.join('tmp',f'word_cloud_{y}_us.png')
            wc.to_file(path_file)
            to_s3(path_file, args.prefix_name, 'output')
            
        logger.info("Step ready...")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto_generate_output', type=int)
    parser.add_argument('--output_uri')
    parser.add_argument('--format_output')
    parser.add_argument('--external_input',type=int)
    parser.add_argument('--input_dependency_from_output_step',type=int)
    parser.add_argument('--from_step')
    parser.add_argument('--input_data')
    parser.add_argument('--name_step')
    parser.add_argument('--description')
    parser.add_argument('--prefix_name')
    args = parser.parse_args()

    description = f'Step: {args.name_step} - {args.description}'
    output = f's3a://{args.prefix_name}/output/{args.output_uri}'

    if args.input_dependency_from_output_step > 0:
        input = f's3a://{args.prefix_name}/output/{args.input_data}'
    elif args.external_input > 0:
        input = args.input_data
    else:
        input = f's3a://{args.prefix_name}/input/{args.input_data}'

    logger.info("Executing: %s.", description)
    logger.info("Input: %s.", input)
    logger.info("Output: %s.", output)
    spark = create_spark_session(description)
    execute_step(spark, input, output, args)