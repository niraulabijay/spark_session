def check_fact_dim_join(fact_df, dim_df, key_col):
    unmatched_df = fact_df.join(
        dim_df,
        fact_df[key_col] == dim_df[key_col],
        "left"
    ).select(fact_df[key_col])
    is_valid = unmatched_df.filter(col("dim_key").isNull()).count() == 0
    return is_valid, unmatched_df