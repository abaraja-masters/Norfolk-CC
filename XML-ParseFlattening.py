import xml.etree.ElementTree as ET
from pyspark.sql import *
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define a UDF to parse XML
def parse_xml(xml_str):
  """
  Parses an XML string into a Spark DataFrame

  Args:
    xml_str: An XML string

  Returns:
    A Spark DataFrame
  """

  root = ET.fromstring(xml_str)
  df = spark.createDataFrame([root.find('.').attrib])
  return df

# Read the XML file into a Spark DataFrame
df = spark.read.text('input_data.xml')

# Parse the XML DataFrame using the UDF
df = df.withColumn('xml_parsed', parse_xml(df['value']))

# Flatten the nested Spark DataFrame
df = df.selectExpr('xml_parsed.*')

# Write the flattened Spark DataFrame to the output
df.write.csv('output_data.csv', header=True, mode='overwrite')
