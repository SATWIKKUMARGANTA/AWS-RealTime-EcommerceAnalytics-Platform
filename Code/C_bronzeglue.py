import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE = "s3://bronze-512888886024-ap-southeast-2-an/clickstream/"
SILVER = "s3://silver-512888886024-ap-southeast-2-an/"

raw_df = spark.read.option("recursiveFileLookup", "true").text(BRONZE)
print(f"Raw lines: {raw_df.count()}")
raw_df.show(5, truncate=False)

def parse_lines(rows):
    import json, csv, io
    for r in rows:
        line = r.value.strip()
        if not line: continue
        try:
            obj = json.loads(line)
            # obj lo header key ye ; tho untadi ma
            for key, val in obj.items():
                if key == "_source": continue
                if ';' in key: # key ye header ma - "order_id";"customer_id"...
                    header = key
                    data_row = val
                    # data_row ni ; tho split chey ma
                    try:
                        reader = csv.reader(io.StringIO(data_row), delimiter=';', quotechar='"')
                        cols = next(reader)
                        cols = [c.strip().strip('"') for c in cols]
                        if 'customer_id' in header.lower() and len(cols) == 7 and cols[0].startswith('O'):
                            yield ('order', cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6])
                        elif 'product_name' in header.lower() and len(cols) == 4 and cols[0].startswith('P'):
                            yield ('product', cols[0], cols[1], cols[2], cols[3])
                    except: continue
                elif isinstance(val, str) and ';' in val:
                    # val lo data undi ma
                    try:
                        reader = csv.reader(io.StringIO(val), delimiter=';', quotechar='"')
                        cols = next(reader)
                        cols = [c.strip().strip('"') for c in cols]
                        if len(cols) == 7 and cols[0].startswith('O'):
                            yield ('order', cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6])
                        elif len(cols) == 4 and cols[0].startswith('P'):
                            yield ('product', cols[0], cols[1], cols[2], cols[3])
                    except: continue
        except:
            # JSON kadu - direct CSV ayite
            import csv, io
            line2 = line.replace('{','').replace('}','')
            try:
                reader = csv.reader(io.StringIO(line2), delimiter=';', quotechar='"')
                cols = next(reader)
                cols = [c.strip().strip('"') for c in cols]
                if len(cols) == 7 and cols[0].startswith('O'):
                    yield ('order', cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6])
                elif len(cols) == 4 and cols[0].startswith('P'):
                    yield ('product', cols[0], cols[1], cols[2], cols[3])
            except: continue

parsed = raw_df.rdd.mapPartitions(parse_lines)
print(f"Parsed count check")
parsed_count = parsed.count()
print(f"Parsed total: {parsed_count}")

orders_rdd = parsed.filter(lambda x: x[0]=='order').map(lambda x: (x[1],x[2],x[3],x[4],x[5],x[6],x[7]))
products_rdd = parsed.filter(lambda x: x[0]=='product').map(lambda x: (x[1],x[2],x[3],x[4]))

print(f"Orders RDD: {orders_rdd.count()}, Products RDD: {products_rdd.count()}")

if orders_rdd.count() > 0:
    orders_df = orders_rdd.toDF(["order_id","customer_id","product_id","amount","order_date","city","payment_method"]).dropDuplicates(["order_id"])
    orders_df.write.mode("overwrite").parquet(SILVER + "orders/")
    print("Orders DONE ma!")
if products_rdd.count() > 0:
    products_df = products_rdd.toDF(["product_id","product_name","category","price"]).dropDuplicates(["product_id"])
    products_df.write.mode("overwrite").parquet(SILVER + "products/")
    print("Products DONE ma!")

job.commit()