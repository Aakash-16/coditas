"""
Inventory procurement strategy:
1.	Review the sales data for last 3 month, so that we get the trending product for upcoming sale.
2.	Avoid backorder (out of stock) product in the sale.
3.	Include overstock product in the sale, we can set a threshold if unit of product is greater than this threshold then it is overstock and it should be mandatorily available in the sale.
4.	Finding out most sold category from previous 3-month sales data and include all product within this category in Big day sale except backorders

"""

from pyspark.sql import SparkSession, DataFrame
import sys
import pyspark.sql.functions

if __name__ == "__main__":

    # Reading Command line arguments for Order.csv and Product.csv
    ORDER_PATH=sys.argv[1]
    PRODUCT_PATH = sys.argv[2]
    OUTPUT_PATH = sys.argv[3]

    # Creating Spark session
    spark = SparkSession \
        .builder \
        .appName("InventoryProcurement") \
        .getOrCreate()

    """ 
     Reading the Order.csv  to create DataFrame.
     
     o/p : 
     +-------+---------+-----------+-----+--------------+--------------------+
     |OrderId|ProductId|DateOfOrder|Units|    OrderOwner|     DeliveryAddress|
     +-------+---------+-----------+-----+--------------+--------------------+
     |  O1231|     1201|  1-12-2018|    1|  Rahul Sharma|23-4b saket nagar...|
     |  O1232|     1204|  1-12-2018|    1|   Rohan Kumar|     magarpatta Pune|
     |  O1233|     1220|  1-12-2018|    1|   Kirti Gupta|Sector 16 Noida D...|
     |  O1234|     1212|  1-12-2018|    1|Neha Choudhary|   B-301 la sallete |
     |  O1235|     1208|  1-12-2018|    1|  Utakarh Nema|56/188 Trupatipur...|
     -------------------------------------------------------------------------
     """
    order_df= spark.read.csv(path=ORDER_PATH,inferSchema=True,header=True)
    order_df.show()
    """
    Finding out the count of each product from previous Order.
    
    o/p:
     +---------+------+
    |ProductId|counts|
    +---------+------+
    |     1212|     1|
    |     1201|     1|
    |     1208|     4|
    |     1206|     1|
    |     1220|     1|
    |     1211|     1|
    |     1209|     1|
    |     1202|     1|
    |     1204|     7|
    +---------+------+
    """

    product_counts_df=order_df.groupBy('ProductId').agg(pyspark.sql.functions.count('*').alias('counts'))

    """
    Reading the Product.csv  to create DataFrame and persisting it because it is used multiple time ,
    this will avoid  creating Product dataframe again and again .
    o/p:
    
    """
    product_df = spark.read.csv(path=PRODUCT_PATH, inferSchema=True, header=True)
    product_df.persist()
    """
    Performing join between  between  product_df  and product_counts_df, so we can get category of the product whose count is greater than 3.
    Here 3 is the threshold value , we are selecting only those product whose counts is greater than threshold i.e 3.
    
    o/p:
     +----------+---------+------+
     |CategoryId|ProductId|counts|
     +----------+---------+------+
     |         8|     1208|     4|
     |         2|     1204|     7|
     +----------+---------+------+

    """
    product_counts_greater3=product_df.join(product_counts_df, product_counts_df.ProductId == product_df.ProductId, 'inner').select(product_df.CategoryId, product_df.ProductId, product_counts_df.counts).filter(product_counts_df.counts > 3)

    """
     Performing join between  product_df and product_counts_greater3 on categoryId so that we can get all product for trending category.
     Also filtering out back order  stocks.
     o/p:
     +---------+-----------+
     |ProductId|ProductName|
     +---------+-----------+
     |     1222|Apple Phone|
     |     1204|      Alexa|
     |     1201|DELL Laptop|
     |     1208| Parker Pen|
     +---------+-----------+
    """
    product_trending_category=product_df.join(product_counts_greater3, product_df.CategoryId == product_counts_greater3.CategoryId, 'inner').select(product_df.ProductId, product_df.ProductName).filter(product_df.Stock != 0)
    """
    Filtering out Over stock product, so that they can be included in the sale .
    We have assumed that if product stock is more than 5 then it is over stock .
    o/p:
    
    +---------+--------------+
    |ProductId|   ProductName|
    +---------+--------------+
    |     1201|   DELL Laptop|
    |     1204|         Alexa|
    |     1208|    Parker Pen|
    |     1209|      BedSheet|
    |     1210|   Girls watch|
    |     1212|      lipstick|
    |     1214|      eyeliner|
    |     1215|    Men Wallet|
    |     1216|Puma men shoes|
    |     1218|         Chair|
    |     1219|         shirt|
    |     1222|   Apple Phone|
    +---------+--------------+
    
    """
    overstock_product_df=product_df.select(product_df.ProductId, product_df.ProductName).filter(product_df.Stock > 5)
    """
    Merging together  trending product and overstock product to get final product list for the sale.
    o/p:
    +---------+--------------+
    |ProductId|   ProductName|
    +---------+--------------+
    |     1222|   Apple Phone|
    |     1204|         Alexa|
    |     1219|         shirt|
    |     1208|    Parker Pen|
    |     1218|         Chair|
    |     1215|    Men Wallet|
    |     1214|      eyeliner|
    |     1210|   Girls watch|
    |     1216|Puma men shoes|
    |     1209|      BedSheet|
    |     1201|   DELL Laptop|
    |     1212|      lipstick|
    +---------+--------------+
    """
    product_expectedin_sale = product_trending_category.union(overstock_product_df).distinct()
    product_expectedin_sale.show()
    """
    Writting output  into csv file for simplicity. 
    """
    product_expectedin_sale.coalesce(2).write.csv(OUTPUT_PATH)
