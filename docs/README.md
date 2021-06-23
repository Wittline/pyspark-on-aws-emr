# Building a Big Data Pipeline with PySpark and Amazon EMR on Spot Fleet and On-Demand Instances

Si eres un cientifico de datos y quieres dar otro paso en tu carrera y convettirte en un cientifico aplicado debes dejar atras los proyectos escolares que involucran trabajar con datasets cortos pequeños o de tamaño medio, la verdadera naturaleza de un cientifico aplicado es saber aprovechar el computo a escala masiva, debes empezar a conocer las tecnologias disponibles para trabajar y procesar grandes conjuntos de datos y es aqui donde los skills en ingenieria de datos empiezan a ser relevantes para dar el siguiente paso en tu carrera, tambien, este nuevo cambio involucra mas responsabilidades tales como: Elegir un proveedor para computo en la nube, Crear arquitecturas escalables y cost-efetive, una estrategia para monitorear tus gastos y recursos, y el tunning de tus recursos etc. El objetivo de este proyecto es ofrecer una plantilla que puedes usar rapidamente si la necesidad de tu analisis involucra trabajar con millones de registros, la plantilla la puedes alterar facilmente para que soporte el tamaño de tu proyecto, y de esta forma no te preocuparas por crear todo dede el inicio y solo enfocarte en escribir pyspark code.

## Architecture - Automate Word Cloud

![alt text](https://wittline.github.io/pyspark-on-aws-emr/Images/architecture.png)

## Data sources
Para poder reproductir el efecto de trabajar con un dataset grande estamos usando el <a href="https://s3.amazonaws.com/amazon-reviews-pds/readme.html"> Amazon Customer Reviews </a> Dataset, y contando las diferentes palabras que tienen los titulos de sus libros comprados, crear nubes de palabras por cada año. Este proyecto no se enfoca en analisis especificos, su objetivo es crear un workflow de big data y conectar sus distintas tareas involucradas usando un cluster de AWS EMR. Podrias usar esta misma plantilla para otro tipo de proyectos o analisis.

## Infrastructure as Code (IaC) in AWS

The aim of this section is to create a EMR cluster on AWS and keep it available for use by the PySpark tasks.

### File structure

#### IAC files
Ya que toda la infraestrucura se crea por codigo, hay varios archivos que fueron modificados para crear este proyecto, puedes conseguir la fuente original de estos en la documentacion de amazon <a href="https://docs.aws.amazon.com/code-samples/latest/catalog/code-catalog-python-example_code-emr.html"> Python Code Samples for Amazon EMR </a>, los archivos de este proyecto son:

1. **ec2.py**: This file helps us create the security groups in our VPC.
2. **iam.py**: This file is for create the two associated roles: AmazonElasticMapReduceRole and AmazonElasticMapReduceforEC2Role.
3. **s3.py**: Control and manage the initial configuration that our S3 bucket needs, scripts, logs, configuration files, etc..
4. **poller.py**: This is checking a function for status each N seconds until reach a specified status.
5. **emr.py**: This file contains the functions to create an emr cluster and add steps to the cluster using boto3.

#### Main process:
6. **emr_process.py**: This file is wrapping the basic functions of the boto3 library, the difference is that it is modified to interpret and understand our configuration files or project template.


#### Configuration files:
7. **cluster-ec2-spot-fleet.json**:
8. **bootstrap-action.sh**:
9. **steps.json**:

#### PySpark code:
10. **pyspark_preprocessing_text.py**:
11. **pyspark_grouping_words.py**:
12. **generate_clouds.py**:

## Running the example

Si no quieres leer los pasos a continuaciòn puedes ver este video de youtube donde te explica como correr el ejemplo paso a paso.

### Steps to follow:

- Create a new User in AWS with *AdministratorAccess** and get your security credentials
- Go to this url: <a href="https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html ">AWS CLI </a> and configure your AWS Credentials in your local machine
- Install <a href="https://www.stanleyulili.com/git/how-to-install-git-bash-on-windows/">git-bash for windows</a>, once installed , open **git bash** and download this repository, this will download all the files needed.

``` 
ramse@DESKTOP-K6K6E5A MINGW64 /c
$ git clone https://github.com/Wittline/pyspark-on-aws-emr.git
```

```
ramse@DESKTOP-K6K6E5A MINGW64 /c/pyspark-on-aws-emr/code
python emr_process.py -a create_cluster -c Project1
```


```
ramse@DESKTOP-K6K6E5A MINGW64 /c/pyspark-on-aws-emr/code
python emr_process.py -a add_steps -steps steps.json -idc j-32XU1B7LJSSRO
``

```
ramse@DESKTOP-K6K6E5A MINGW64 /c/pyspark-on-aws-emr/code
python emr_process.py -a execute_steps -idc j-32XU1B7LJSSRO
``


## Word clouds of book titles purchased from Amazon USA (1995 - 2015)

![word_clouds](https://user-images.githubusercontent.com/8701464/123040480-6d3c2200-d3b9-11eb-864c-054d35e0156a.gif)

## Contributing and Feedback
Any ideas or feedback about this repository?.
<br>
You can contact me on <a href="https://twitter.com/RamsesCoraspe" target="_blank">Twitter</a> | <a href="https://www.linkedin.com/in/ramsescoraspe/" target="_blank">LinkedIn</a>

## Authors
- Created by <a href="https://www.linkedin.com/in/ramsescoraspe"><strong>Ramses Alexander Coraspe Valdez</strong></a>
- Created on June 2021

## License
This project is licensed under the terms of the Apache License.



