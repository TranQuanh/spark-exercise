from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DateType, StringType, IntegerType
from pyspark.sql import SparkSession
import logging
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data = [
  Row(id = 1, value = 28.3, date = date(2021,1,1)),
  Row(id = 2, value = 15.8, date = date(2021,1,1)),
  Row(id = 3, value = 20.1, date = date(2021,1,2)),
  Row(id = 4, value = 12.6, date = date(2021,1,3))
]
print("tạo bảng theo ROW")
# do đã có tên cột nên chỉ cần đưa data vào
df = spark.createDataFrame(data)
print()
print(type(df))
print()
print(df)
df.show()
print("tạo bảng theo list bình thường")

data = [
  (12114, 'Anne', 21, 1.56, 8, 9, 10, 9, 'Economics', 'SC'),
  (13007, 'Adrian', 23, 1.82, 6, 6, 8, 7, 'Economics', 'SC'),
  (10045, 'George', 29, 1.77, 10, 9, 10, 7, 'Law', 'SC'),
  (12459, 'Adeline', 26, 1.61, 8, 6, 7, 7, 'Law', 'SC'),
  (10190, 'Mayla', 22, 1.67, 7, 7, 7, 9, 'Design', 'AR'),
  (11552, 'Daniel', 24, 1.75, 9, 9, 10, 9, 'Design', 'AR')
]
# do nó không có dữ liệu nên cần phải có
columns = [
  'StudentID', 'Name', 'Age', 'Height', 'Score1',
  'Score2', 'Score3', 'Score4', 'Course', 'Department'
]
students = spark.createDataFrame(data,columns)

print(students)
students.show()
print()
students.show(2)
print()
print("các cột của table")
print(students.columns)

print("Schema")
print(students.printSchema())

print("schema succint")
schema = students.schema
for sc in schema:
    print(sc)
    print(sc.name)
    print(sc.dataType)
    print(sc.nullable)
    # true false là đại diện cho có null hay ko

print("nếu muốn tự tạo schema riêng")
data = [
  (1, date(2022, 1, 1), 'Anne'),
  (2, date(2022, 1, 3), 'Layla'),
  (3, date(2022, 1, 15), 'Wick'),
  (4, date(2022, 1, 11), 'Paul')
]

schema = StructType([
  StructField('ID', IntegerType(), True),
  StructField('Date', DateType(), True),
  StructField('Name', StringType(), True)
])

registers = spark.createDataFrame(data, schema = schema)
print(registers.schema)
print("muốn kiểm tra cột ID có đúng là kiểu IntegerType() hay không")
schema = registers.schema
id_col = schema[0]

if(isinstance(id_col.name,IntegerType)):
    print('Yes')
else: print("NOs")