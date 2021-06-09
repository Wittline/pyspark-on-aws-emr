import argparse
import logging
import sparknlp
from sparknlp.base import Finisher, DocumentAssembler
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def execute_step(description, input, output):

    with SparkSession.builder.appName(description)\
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp-spark23_2.12:3.0.3")\
        .getOrCreate() as spark:
        
        
        df = spark.read.parquet(input)
        query = df.select("product_title").distinct()

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

        new_text = pipeline.fit(query).transform(query)
        new_text.columns
        new_text.select('finished_clean_lemma')
        new_text_words= new_text.withColumn('exploded_text', f.explode(f.split(f.col('finished_clean_lemma'), ' ')))\
                                .groupBy('exploded_text')\
                                .count()\
                                .sort('count', ascending=False)
        
        counts_pd = new_text_words.toPandas()
        counts_pd

                
        query.write.mode('overwrite').csv(counts_pd)

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