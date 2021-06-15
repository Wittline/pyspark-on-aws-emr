import argparse
import sparknlp
from sparknlp.base import Finisher, DocumentAssembler
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


def create_spark_session(description):

    print("Reading spark object")
    spark = SparkSession\
            .builder\
            .appName(description)\
            .getOrCreate()
    
    print("Spark object ready")
    return spark

def execute_step(spark, input, output):

        print("Executing step...")
        df = spark.read.parquet(input)
        df = df.drop_duplicates()
        
        documentAssembler = DocumentAssembler().setInputCol('product_title').setOutputCol('document')
        tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('token')
        normalizer = Normalizer().setInputCols(['token']).setOutputCol('normalized').setLowercase(True)
        lemmatizer = LemmatizerModel.pretrained().setInputCols(['normalized']).setOutputCol('lemma')
        stop_words = StopWordsCleaner.pretrained('stopwords_en', 'en').setInputCols(["lemma"]).setOutputCol("clean_lemma").setCaseSensitive(False)
        finisher = Finisher().setInputCols(['clean_lemma']).setCleanAnnotations(False)

        
        pipeline = Pipeline().setStages([
                            documentAssembler,
                            tokenizer,
                            normalizer,
                            lemmatizer,
                            stop_words,
                            finisher
                            ])

        print("Executing spark-nlp pipeline...")
        new_text = pipeline.fit(df).transform(df)
        print("Expanding column...")
        new_text_clean = df.withColumn("exploded_text", f.explode(f.col("finished_clean_lemma")))
        print("Saving output...")
        new_text_clean.write.parquet(output)
        print("Step ready...")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--auto_generate_output', type=int)
    parser.add_argument('--output_uri')
    parser.add_argument('--format_output')
    parser.add_argument('--external_input',type=int)
    parser.add_argument('--input_dependency_from_output_step',type=int)
    parser.add_argument('--from_step')
    parser.add_argument('--input_data')
    parser.add_argument('--name')
    parser.add_argument('--description')
    parser.add_argument('--prefix_name')
    args = parser.parse_args()

    description = f'Step: {args.name} - {args.description}'
    output = f's3a://{args.prefix_name}/output/{args.output_uri}'

    if args.input_dependency_from_output_step > 0:
        input = f's3a://{args.prefix_name}/output/{args.input_data}'
    elif args.external_input > 1:
        input = args.input_data
    else:
        input = f's3a://{args.prefix_name}/input/{args.input_data}'

    print("Executing: %s.", description)
    print("Input: %s.", input)
    print("Output: %s.", output)
    spark = create_spark_session(description)
    execute_step(spark, input, output)