import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

import csv
from faker import Faker


def create_file_data(csv_file):
	fake = Faker()

	try:
		count = int(input("Enter count of rows (min=1000): "))
		if count < 1000:
			raise ValueError()
	except ValueError:
		print("Error: Count must be at least 1000!\nThe default value is set 1000")
		count = 1000

	with open(csv_file, mode="w", newline="") as file:
		writer = csv.writer(file)
		writer.writerow(["Date", "UserId", "Product", "Count", "Price"])

		for i in range(0, count):
			writer.writerow(
				[
					fake.date_this_year(),
					fake.random_int(min=1, max=1000),
					fake.word(ext_word_list=['Steel', 'Cotton', 'Wheat', 'Leather', 'Plastic']),
					fake.random_int(min=1, max=100),
					fake.random_int(min=100, max=1000)/10
				]
			)
	print("\nThe file is recorded\n")


spark = SparkSession.builder \
    .appName("example") \
    .getOrCreate()

csv_file_path = "fake_data.csv"

flag_create = True
if os.path.exists(csv_file_path):
	create = input("File with data is exists\n"
				   "Recreate file with data?\n"
				   "Enter [y]-to create or any symbol to continue... ")

	if (create !='y'):
		flag_create = False
else:
	print("File with data is not exists")

if flag_create:
	create_file_data(csv_file_path)

print("Loading data from file... waiting...")
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
df.show()
spark.stop()