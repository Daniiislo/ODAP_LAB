from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, desc
import os

def create_spark_session(app_name=""):
    """
    Create a Spark session
    
    Args:
        app_name (str):Name of the Spark application
    
    Returns:
        SparkSession: A Spark session object
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def clear_console():
    """
    Clear the console output base on the operating system.
    """
    import platform
    
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')
    
def Ex1_read_and_display_data(spark, file_path):
    """
    Read data from a CSV file and display the first 5 rows
    
    Args:
        spark (SparkSession): A Spark session
        file_path (str): Path to the CSV file
        
    Returns:
        DataFrame: The loaded DataFrame
    """

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print("First 5 rows of the data:")
    df.show(5)
    return df

def Ex2_filter_and_sort_data(df):
    """
    Filter products with price > 100 and then sort by price in descending order
    
    Args:
        df (DataFrame): Input DataFrame
    Returns:
        DataFrame: Filtered and sorted DataFrame
    """
    filtered_df_sorted_by_price_desc = df.filter(col("Price") > 100).orderBy(desc("Price"))
    return filtered_df_sorted_by_price_desc

def Ex3_calculate_total_and_average_price(df):
    """
    Calculate the total and average price of products in the DataFrame

    Args:
        df (DataFrame): Input DateFrame

    Returns:
        tuple: Total price and Average price
    """
    result_df = df.select(sum("price").alias("Total Price"), avg("Price").alias("Average Price"))
    total_price = result_df.collect()[0]["Total Price"]
    average_price = result_df.collect()[0]["Average Price"]
    return total_price, average_price

def Ex4_add_new_column_total_price(df):
    """
    Add a new column 'Total Price' by multiplying price and quantity

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: DataFrame with the new column
    """
    df_with_total_price = df.withColumn("Total Price", col("Price") * col("Quantity"))
    return df_with_total_price

def Ex5_group_by_category_and_calculate_total_quantity(df):
    """
    Group products by category and calculate the total quantity in each category

    Args:
        df (DataFrame): Input DataFrame

    Returns:
        DataFrame: DataFrame with total quantity by category
    """
    category_totals = df.groupBy("Categories").agg(sum("Quantity").alias("Total Quantity"))
    return category_totals


def main():
    spark = create_spark_session("TH5-PySpark")
    clear_console()

    # Get file path from user
    file_path = input("Enter the path to the CSV file: ")

    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    print(f"Reading data from: {file_path}")

    print("\n" + "=" * 50)
    # Read and display first 5 rows of the data
    df = Ex1_read_and_display_data(spark, file_path)

    print("\n" + "=" * 50)
    # Filter products with price > 100 and sort by price in descending order
    filtered_df_sorted = Ex2_filter_and_sort_data(df)
    print("Filtered and sorted products (Price > 100) (The first 50 rows):")
    filtered_df_sorted.show(50)

    print("\n" + "=" * 50)
    # Calculate total and average price
    total_price, average_price = Ex3_calculate_total_and_average_price(df)
    print(f"Total Price: {total_price}, Average Price: {average_price}")

    print("\n" + "=" * 50)
    # Add a new column 'Total Price' by multiplying price and quantity
    df_with_total_price = Ex4_add_new_column_total_price(df)
    print("Data with Total Price column (The first 50 rows):")
    df_with_total_price.show(50)

    print("\n" + "=" * 50)
    # Group by category and calculate total quantity
    category_totals = Ex5_group_by_category_and_calculate_total_quantity(df)
    print("Total quantity by category:")
    category_totals.show(50)

    print("\n" + "=" * 50)
    # Stop the Spark session
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()

