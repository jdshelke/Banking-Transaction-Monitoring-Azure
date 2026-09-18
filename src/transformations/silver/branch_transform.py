from src.utils import data_quality


def transform(df):
    drop_clumns_df = data_quality.drop_columns(df, ["_rescued_data"])

    drop_null_df = data_quality.drop_null_records(drop_clumns_df, ["branch_id", "ifsc_code"])

    # dedup_df = data_quality.remove_duplicates(drop_null_df, "branch_id")

    trim_string_df = data_quality.trim_columns(drop_null_df, ["branch_name", "city", "ifsc_code"])

    to_upper_case_df = data_quality.change_case(trim_string_df, ["state", "ifsc_code"], "upper")

    date_valid_df = data_quality.validate_date(to_upper_case_df, "opened_date")

    cleaned_branch_df = data_quality.fill_null_value(date_valid_df, "Unknown", ["branch_name", "city", "state"])

    # print("For Branch Total Records Processed: ", cleaned_branch_df.count())

    return cleaned_branch_df
