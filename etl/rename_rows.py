from pyspark.sql.functions import regexp_replace

def rename_rows(dataframe, column, regxs: list):
    """regxs -> It's a list that contain a list with regexpression(index 0) and the new content in that row
        [[r"", "new_line"]]"""
    df_temp = dataframe
    for reg in regxs:
        df_temp = df_temp.withColumn(column, regexp_replace(column, reg[0], reg[1]))
    return df_temp