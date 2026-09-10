from pyspark.sql.functions import col, trim, lower, upper, current_date
from functools import reduce
from operator import and_

def remove_duplicates(df, columns):
    return df.dropDuplicates([*columns])

def drop_null_records(df, columns):
    return df.dropna(subset=[*columns])

def trim_columns(df, columns):
    for column in columns:
        df = df.withColumn(column, trim(col(column)))
    return df

def fill_null_value(df, value, columns):
    return df.fillna(value, subset=[*columns])

def change_case(df, columns, case_type):
    if case_type.lower() == "lower":
        for column in columns:
            df = df.withColumn(column, lower(col(column)))
        return df
    if case_type.lower() == "upper":
        for column in columns:
            df = df.withColumn(column, upper(col(column)))
        return df

def to_timestamp(df, columns):
    for column in columns:
        df = df.withColumn(column, col(column).cast("timestamp"))
    return df

def validate_non_negative(df, columns):
    conditions = [
        col(column).isNotNull() & (col(column) >= 0)
        for column in columns
    ]

    return df.filter(reduce(and_, conditions))

def validate_date(df, column):
    return df.filter(
        (col(column) <= current_date())
    )

def validate_boolean(df, column):
    return df.filter(
        col(column).isin(True, False)
    )

def validate_date_relationship(df, start_column, end_column):
    return df.filter(
        col(start_column).isNotNull() &
        (
            col(end_column).isNull() |
            (col(end_column) >= col(start_column))
        )
    )