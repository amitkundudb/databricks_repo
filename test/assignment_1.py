import unittest
from pyspark.sql import SparkSession
from src.assignment_1.source_to_bronze.utils import (
    camel_to_snake,
    current_date_df,
    salary_of_each_department,
    employee_count,
    list_the_department,
    avg_age,
    add_load_date,
)

class TestSparkFunctions(unittest.TestCase):
    df = None
    data_path = None
    schema = None
    sample_data = None
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestSparkFunctions").getOrCreate()
        cls.data_path = "/resource/employee_q1.csv"
        cls.schema = "EmployeeID INT, EmployeeName STRING, Department STRING, Country STRING, Salary INT, Age INT"
        cls.sample_data = [
            (1, "James", "D101", "IN", 9000, 25),
            (2, "Michel", "D102", "SA", 8000, 26),
            (3, "James son", "D101", "IN", 10000, 35),
            (4, "Robert", "D103", "MY", 11000, 34),
            (5, "Scott", "D104", "MA", 6000, 36),
            (6, "Gen", "D105", "JA", 21345, 24),
            (7, "John", "D102", "MY", 87654, 40),
            (8, "Maria", "D105", "SA", 38144, 38),
            (9, "Soffy", "D103", "IN", 23456, 29),
            (10, "Amy", "D103", "CN", 21345, 24),
        ]
        cls.df = cls.spark.createDataFrame(cls.sample_data, schema=cls.schema)

    def test_camel_to_snake(self):
        df_snake_case = camel_to_snake(self.df)
        # Assert that column names are converted to snake case
        self.assertEqual(df_snake_case.columns, [col_name.lower() for col_name in self.df.columns])

    def test_current_date_df(self):
        df_with_current_date = current_date_df(self.df)
        # Assert that the DataFrame has a 'load_date' column with the current date
        self.assertTrue("load_date" in df_with_current_date.columns)

    def test_salary_of_each_department(self):
        salary_df = salary_of_each_department(self.df)
        # Assert that the DataFrame contains the correct number of rows
        self.assertEqual(salary_df.count(), 5)

    def test_employee_count(self):
        employees_count_df = employee_count(self.df)
        # Assert that the DataFrame contains the correct number of rows
        self.assertEqual(employees_count_df.count(), 10)

    def test_list_the_department(self):
        dept_and_country_df = list_the_department(self.df)
        # Assert that the DataFrame contains the correct number of rows
        self.assertEqual(dept_and_country_df.count(), 5)

    def test_avg_age(self):
        avg_age_df = avg_age(self.df)
        # Assert that the DataFrame contains the correct number of rows
        self.assertEqual(avg_age_df.count(), 5)

    def test_add_load_date(self):
        df_with_load_date = add_load_date(self.df)
        # Assert that the DataFrame has a 'at_load_date' column with the current date
        self.assertTrue("at_load_date" in df_with_load_date.columns)

if __name__ == "__main__":
    unittest.main()