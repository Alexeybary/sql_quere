import null as null
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, concat, concat_ws
from pyspark.sql.functions import col,when,count, sum
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType


def create_df(name):
    return spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/pagila")\
    .option("driver", "org.postgresql.Driver").option("dbtable", name)\
    .option("user", "postgres").option("password", "new_password").load()
spark = SparkSession.builder.config("spark.jars", "/home/alexey/Downloads/postgresql-42.2.5.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()

df_film=create_df("film")
df_category=create_df("category")
df_film_category=create_df("film_category")
df_actor=create_df("actor")
df_film_actor=create_df("film_actor")
df_inventory=create_df("inventory")
df_rental=create_df("rental")
df_payment=create_df("payment")
df_city=create_df("city")
df_address=create_df("address")
df_customer=create_df("customer")

print("first_task")

df_temp=df_film.join(df_film_category,df_film_category.film_id==df_film.film_id,how='inner')\
    .join(df_category,df_film_category.category_id==df_category.category_id,how='inner')\
    .groupBy("name").count().orderBy("count",ascending=False)
df_temp.show()
print("second")

df_actor.join(df_film_actor,df_actor.actor_id==df_film_actor.actor_id,how='inner')\
    .join(df_inventory,df_film_actor.film_id==df_inventory.film_id,how='inner')\
    .join(df_rental,df_inventory.inventory_id==df_rental.inventory_id,how="inner")\
    .withColumn('actor_name',concat_ws(" ",df_actor.first_name,df_actor.last_name))\
    .groupBy("actor_name").count().orderBy("count",ascending=False).show(10)
print("third")

df_category.join(df_film_category,df_category.category_id==df_film_category.category_id,how="inner")\
    .join(df_inventory,df_inventory.film_id==df_film_category.film_id,how="inner")\
    .join(df_rental,df_inventory.inventory_id==df_rental.inventory_id,how="inner")\
    .join(df_payment,df_rental.rental_id==df_payment.rental_id,how="inner")\
    .groupBy("name").sum("amount").withColumnRenamed('sum(amount)', 'sum').orderBy("sum",ascending=False).show(1)
print("fourth")
df_temp=df_film.join(df_inventory,df_inventory.film_id==df_film.film_id,how="left")
df_temp.filter(df_temp.inventory_id.isNull()).select(df_temp.title,df_film.film_id).show(100)
print("fifth")
df_temp=df_actor.join(df_film_actor,df_film_actor.actor_id==df_actor.actor_id,how='inner')\
    .join(df_film_category,df_film_category.film_id==df_film_actor.film_id,how='inner')\
    .join(df_category,df_film_category.category_id==df_category.category_id,how='inner')\
    .withColumn('actor_name',concat_ws(" ",df_actor.first_name,df_actor.last_name))\
    .filter(df_category.name=='Children').groupBy('actor_name').count().orderBy("count",ascending=False)
df_temp2=df_temp.select('count').limit(3).rdd.flatMap(lambda x: x).collect()
df_temp.select("actor_name","count").filter(col("count").isin(df_temp2)).show()
print("sixth")
df_city.join(df_address,df_address.city_id==df_city.city_id,how='inner')\
    .join(df_customer,df_customer.address_id==df_address.address_id,how='inner')\
    .groupBy('city').agg(count(when(col('active')==1,True)).alias("active_people"),
                         count(when(col('active')==0,True)).alias("not_active")).orderBy("not_active",ascending=False).show()
print("seventh")
df_rental=df_rental.filter(~df_rental.return_date.isNull())
df_time=df_city.join(df_address,df_address.city_id==df_city.city_id,how='inner')\
    .join(df_customer,df_customer.address_id==df_address.address_id,how='inner')\
    .join(df_rental,df_rental.customer_id==df_customer.customer_id,how="inner")\
    .join(df_inventory,df_inventory.inventory_id==df_rental.inventory_id,how="inner")\
    .join(df_film,df_film.film_id==df_inventory.film_id,how="inner")\
    .join(df_film_category,df_film_category.film_id==df_film.film_id,how='inner')\
    .join(df_category,df_category.category_id==df_film_category.category_id,how='inner')\
    .withColumn("time",(df_rental.return_date-df_rental.rental_date))\
    .select("title","time","name","city")
df_time.where((col('title').like("A%"))).groupBy("name").agg(sum(col('time'))).orderBy("sum(time)",ascending=False).limit(1).show()
df_time.where((col('city').like("%-%"))).groupBy("name").agg(sum(col('time'))).orderBy("sum(time)",ascending=False).limit(1).show()
