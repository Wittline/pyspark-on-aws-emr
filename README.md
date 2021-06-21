# PySpark-on-AWS-EMR

# Building a Big Data Pipeline with PySpark and Amazon EMR on Spot Fleet and On-Demand Instances

Si eres un cientifico de datos y quieres dar otro paso en tu carrera y convettirte en un cientifico aplicado debes dejar atras los proyectos escolares que involucran trabajar con datasets cortos pequeños o de tamaño medio, la verdadera naturaleza de un cientifico aplicado es saber aprovechar el computo a escala masiva, debes empezar a conocer las tecnologias disponibles para trabajar y procesar grandes conjuntos de datos y es aqui donde los skills en ingenieria de datos empiezan a ser relevantes para dar el siguiente paso en tu carrera, tambien, este nuevo cambio involucra mas responsabilidades tales como: Elegir un proveedor para computo en la nube, Crear arquitecturas escalables y cost-efetive, una estrategia para monitorear tus gastos y recursos, y el tunning de tus recursos etc. El objetivo de este proyecto es ofrecer una plantilla que puedes usar rapidamente si la necesidad de tu analisis involucra trabajar con millones de registros, la plantilla la puedes alterar facilmente para que soporte el tamaño de tu proyecto, y de esta forma no te preocuparas por crear todo dede el inicio y solo enfocarte en escribir pyspark code.

# Automate Word Cloud

# Data sources
Para poder reproductir el efecto de trabajar con un dataset grande estamos usando el "Amazon Customer Reviews" Dataset, y contando las diferentes palabras que tienen los titulos de sus libros comprados, crear nubes de palabras por cada año. Este proyecto no se enfoca en analisis especificos, su objetivo es crear un workflow de big data y conectar sus distintas tareas involucradas usando un cluster de AWS EMR. Podrias usar esta misma plantilla para otro tipo de proyectos o analisis.

https://s3.amazonaws.com/amazon-reviews-pds/readme.html


# Infrastructure as Code (IaC) in AWS

The aim of this section is to create a EMR cluster on AWS and keep it available for use by the PySpark tasks.

## File structure

### IAC files
Ya que toda la infraestrucura se crea por codigo, hay varios archivos que fueron modificados para crear este proyecto, puedes conseguir la fuente original de estos en la documentacion de amazon "Python Code Samples for Amazon EMR", los archivos de este proyecto son:
- ec2.py
- iam.py
- s3.py
- poller.py
- emr.py

### Main process:
- emr_process.py

### Configuration files:
- cluster-ec2-spot-fleet.json
- bootstrap-action.sh
- steps.json

### PySpark code:
- pyspark_preprocessing_text.py
- pyspark_grouping_words.py
- generate_clouds.py

# Running the example

Si no quieres leer los pasos a continuaciòn puedes ver este video de youtube donde te explica como correr el ejemplo paso a paso.

## Steps to follow:

- Install <a href="https://www.stanleyulili.com/git/how-to-install-git-bash-on-windows/">git-bash for windows</a>, once installed , open **git bash** and download this repository, this will download all the files needed.

``` 
ramse@DESKTOP-K6K6E5A MINGW64 /c
$ git clone https://github.com/Wittline/PySpark-on-AWS-EMR.git

```

















Spot Fleet and On Demand Instances

Create EMR
S3, VPC, IAM




