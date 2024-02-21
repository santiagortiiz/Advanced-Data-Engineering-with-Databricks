# Databricks notebook source
from dbacademy import dbgems

class StreamFactory:
    """
    Incrementally loads data from a source dataset to a target directory.

    Attributes:
        source_dir: source path for datasets
        target_dir: landing path for streams
        max_batch: total number of batches before exhausting stream
        batch: counter used to track current batch number

    Methods:
        load(continuous=False)
        load_batch(target, batch, end): dataset-specific function provided at instantiation
    """
    def __init__(self, source_dir, target_dir, load_batch, max_batch):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.load_batch = load_batch
        self.max_batch = max_batch
        self.batch = 1     
        
    def load(self, continuous=False):
        
        if self.batch > self.max_batch:
            print("Data source exhausted", end="...")
            total = 0                
        elif continuous == True:
            print(f"Loading all batches to the stream", end="...")
            total = self.load_batch(self.source_dir, self.target_dir, self.batch, self.max_batch)
            self.batch = self.max_batch + 1
        else:
            print(f"Loading batch #{self.batch} to the stream", end="...")
            total = self.load_batch(self.source_dir, self.target_dir, self.batch, self.batch)
            self.batch = self.batch + 1
            
        print(f"Loaded {total:,} records")

None

# COMMAND ----------

def load_user_reg_batch(datasets_dir, target_dir, batch_start, batch_end):

    source_dataset = f"{datasets_dir}/user-reg"
    target_path = f"{target_dir}/user_reg"

    df = (spark.read
          .format("json")
          .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
          .load(source_dataset)
          .withColumn("date", F.col("registration_timestamp").cast("timestamp").cast("date"))
          .withColumn("batch", F.when(F.col("date") < "2019-12-01", F.lit(1)).otherwise(F.dayofmonth(F.col("date"))+1))
          .filter(f"batch >= {batch_start}")
          .filter(f"batch <= {batch_end}")          
          .drop("date", "batch")
          .cache())

    df.write.mode("append").format("json").save(target_path)
    return df.count()        


def load_cdc_batch(datasets_dir, target_dir, batch_start, batch_end):
    
    source_dataset = f"{datasets_dir}/pii/raw"
    target_path = f"{target_dir}/cdc"

    df = (spark.read
      .load(source_dataset)
      .filter(f"batch >= {batch_start}")
      .filter(f"batch <= {batch_end}")
    )   
    df.write.mode("append").format("json").save(target_path)
    return df.count()


def load_daily_batch(datasets_dir, target_dir, batch_start, batch_end):
    
    source_path = f"{datasets_dir}/bronze"
    target_path = f"{target_dir}/daily"


    df = (spark.read
      .load(source_path)
      .withColumn("day", 
        F.when(F.col("date") <= '2019-12-01', 1)
        .otherwise(F.dayofmonth("date")))
      .filter(F.col("day") >= batch_start)
      .filter(F.col("day") <= batch_end)
      .drop("date", "week_part", "day")  
    )
    df.write.mode("append").format("json").save(target_path)
    return df.count()

None

