"""drugs generation."""

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

from pyspark_base.utils.readers import read_csv
from pyspark_base.utils.spark_config import get_spark_session
from pyspark_base.demo.transformers.common import prefix_cols, prepared_title_array


def drugs_gen(drugs: str, pubmed: str, clinicals_trials: str, output: str) -> None:
    spark: SparkSession = get_spark_session()

    # Read data
    drugs_df: DataFrame = read_csv(spark, drugs)
    prepared_pubmed: DataFrame = (
        read_csv(spark, pubmed)
        .transform(lambda df: prefix_cols(df, "pubmed"))
        .transform(lambda df: prepared_title_array(df, "pubmed_title"))
    )
    prepared_clinical_trials: DataFrame = (
        read_csv(spark, clinicals_trials)
        .transform(lambda df: prefix_cols(df, "clinical_trials"))
        .transform(
            lambda df: prepared_title_array(df, "clinical_trials_scientific_title")
        )
    )

    # Transform data
    journals: DataFrame = (
        drugs_df.join(
            prepared_pubmed,
            f.array_contains(f.col("pubmed_title_array"), f.upper(f.col("drug"))),
            "outer",
        )
        .join(
            prepared_clinical_trials,
            f.array_contains(
                f.col("clinical_trials_scientific_title_array"), f.upper(f.col("drug"))
            ),
            "left",
        )
        .groupby("atccode", "drug")
        .agg(
            f.collect_set(
                f.struct(
                    f.col("pubmed_id"),
                    f.col("pubmed_title"),
                    f.col("pubmed_date"),
                    f.col("pubmed_journal"),
                )
            ).alias("pubmeds"),
            f.collect_set(
                f.struct(
                    f.col("clinical_trials_id"),
                    f.col("clinical_trials_scientific_title"),
                    f.col("clinical_trials_date"),
                    f.col("clinical_trials_journal"),
                )
            ).alias("clinical_trials"),
            f.collect_set(
                f.struct(
                    f.col("pubmed_date").alias("date"),
                    f.col("pubmed_journal").alias("journal"),
                )
            ).alias("journals_pubmed"),
            f.collect_set(
                f.struct(
                    f.col("clinical_trials_date").alias("date"),
                    f.col("clinical_trials_journal").alias("journal"),
                )
            ).alias("journals_clinical_trials"),
        )
        .withColumn(
            "journals", f.array_union("journals_pubmed", "journals_clinical_trials")
        )
        .drop("journals_pubmed", "journals_clinical_trials")
    )
    journals.show()
#   journals.write.mode("overwrite").format("delta").save(journalPath)


