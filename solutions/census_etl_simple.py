import json
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

RAW_SCHEMA = T.StructType([
    T.StructField(name, T.LongType())
    for name in ["year", "age", "females", "males"]
])

# Age group definitions: (name, age_from, age_to)
AGE_GROUPS = [
    ("Children", 0, 14),
    ("Adolescents", 15, 17),
    ("Working Age", 18, 64),
    ("Elderly", 65, 100),
]



def extract(source, warehouse, batch_id):
    """Copy a JSONL source to Bronze without overwriting an existing batch."""
    bronze_path = Path(warehouse) / "bronze" / f"{batch_id}.json"
    if bronze_path.exists():
        return "already_exists"

    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_bytes(Path(source).read_bytes())
    return "committed"


def transfer(spark, warehouse, year, batch_id):
    """Validate a Bronze batch and transfer it through staging to Silver.
       Adds total, batch_id, version_at, and recorded_at columns to the Silver data.
    Args:
        spark: SparkSession
        warehouse: Path to the warehouse directory
        year: Census year (e.g., 2010)
        batch_id: Batch identifier (e.g., "census_2010")
    Returns:
        "committed" on success, "already_exists" if already processed
    """
    warehouse = Path(warehouse)
    version_at = datetime(year, 12, 31, 23, 59, 59).isoformat() + "Z"

    bronze_path = warehouse / "bronze" / f"{batch_id}.json"
    silver_dir = warehouse / "silver" / batch_id
    staging_dir = warehouse / "staging" / batch_id
    quarantine_dir = warehouse / "quarantine" / batch_id
    
    # If data already in Silver: idempotent
    if silver_dir.exists() and list(silver_dir.glob("*.parquet")):
        return "already_exists"
    
    # 2. Read JSONL data from bronze
    raw = spark.read.schema(RAW_SCHEMA).json(str(bronze_path))
    
    # 3. Validation
    count = raw.count()
    ages = raw.select("age").distinct().count()
    
    if count != 101 or ages != 101:
        # Store failed data in QUARANTINE
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        raw.coalesce(1).write.mode("overwrite").json(str(quarantine_dir))
        raise ValueError(f"Validation failed: {count} rows, {ages} ages (expected 101/101)")
    
    # 4. STAGING: Processing workspace for transformations
    staging_dir.mkdir(parents=True, exist_ok=True)
    raw.write.mode("overwrite").parquet(str(staging_dir / "data"))

    # 5. SILVER: Cleaned, validated and versioned with metadata
    silver_dir.mkdir(parents=True, exist_ok=True)
    enriched = raw.withColumn(
        "total", F.col("females") + F.col("males")
    ).withColumn(
        "batch_id", F.lit(batch_id)
    ).withColumn(
        "version_at", F.lit(version_at).cast("timestamp")
    ).withColumn(
        "recorded_at", F.current_timestamp()
    ) 
    enriched.write.mode("overwrite").parquet(str(silver_dir))
    
    return "committed"


def load_simple(spark, source, warehouse, year, batch_id):
    """Extract and transfer a batch, retaining the original combined interface."""
    silver_dir = Path(warehouse) / "silver" / batch_id
    if silver_dir.exists() and list(silver_dir.glob("*.parquet")):
        return "already_exists"

    extract(source, warehouse, batch_id)
    return transfer(spark, warehouse, year, batch_id)


def create_gold_demographics(spark, warehouse, batch_id):
    """
    Create aggregated demographic views in Gold layer from Silver data.
    
    Aggregates population data by age groups and calculates gender metrics:
    - Age groups: Children, Adolescents, Working Age, Elderly
    - Metrics: females, males, total, gender_gap_absolute, gender_gap_relative
    
    Age groups:
      - Children (0-14 years)
      - Adolescents (15-17 years)
      - Working Age (18-64 years)
      - Elderly (65-100 years)
    
    Args:
        spark: SparkSession
        warehouse: Path object to warehouse directory
        batch_id: Batch identifier (e.g., "census_2010")
        
    Returns:
        "committed" on success, "already_exists" if already processed
    """
    warehouse = Path(warehouse) if not isinstance(warehouse, Path) else warehouse
    
    silver_path = warehouse / "silver" / batch_id
    gold_dir = warehouse / "gold" / batch_id
    
    # If already processed: idempotent
    if gold_dir.exists() and list(gold_dir.glob("*.parquet")):
        return "already_exists"
    
    # Read Silver data
    silver = spark.read.parquet(str(silver_path))
    
    # Build CASE-WHEN expression for age groups dynamically
    age_group_expr = None
    for group_name, age_from, age_to in AGE_GROUPS:
        condition = (F.col("age") >= age_from) & (F.col("age") <= age_to)
        if age_group_expr is None:
            age_group_expr = F.when(condition, group_name)
        else:
            age_group_expr = age_group_expr.when(condition, group_name)
    
    # Add age group column
    with_age_group = silver.withColumn("age_group", age_group_expr)
    
    # Aggregate by age group
    aggregated = with_age_group.groupBy("year", "age_group", "batch_id", "version_at").agg(
        F.sum("females").alias("females"),
        F.sum("males").alias("males"),
        F.sum("total").alias("total"),
    )
    
    # Add age_from and age_to columns based on age_group
    age_from_expr = None
    age_to_expr = None
    for group_name, age_from, age_to in AGE_GROUPS:
        condition = F.col("age_group") == group_name
        if age_from_expr is None:
            age_from_expr = F.when(condition, F.lit(age_from))
            age_to_expr = F.when(condition, F.lit(age_to))
        else:
            age_from_expr = age_from_expr.when(condition, F.lit(age_from))
            age_to_expr = age_to_expr.when(condition, F.lit(age_to))
    
    demographics = aggregated.withColumn("age_from", age_from_expr).withColumn("age_to", age_to_expr)
    
    # Calculate gender metrics
    demographics = demographics.withColumn(
        "gender_gap_absolute", F.col("females") - F.col("males")
    ).withColumn(
        "gender_gap_relative", 
        F.round(100.0 * (F.col("females") - F.col("males")) / F.col("total"), 2)
    ).withColumn(
        "recorded_at", F.current_timestamp()
    ).select(
        "year", "age_group", "age_from", "age_to", "batch_id", "version_at",
        "females", "males", "total", "gender_gap_absolute", "gender_gap_relative", "recorded_at"
    )
    
    # Write to Gold layer
    gold_dir.mkdir(parents=True, exist_ok=True)
    demographics.coalesce(1).write.mode("overwrite").parquet(str(gold_dir))
    
    return "committed"
