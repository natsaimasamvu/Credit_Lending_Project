#Library of helper functions which are repeatedly used in the DataPipeline
def read_data(spark, file_path: str, file_format: str) -> 'DataFrame':
    """
    Reads data from a file path and returns a DataFrame.
    :param file_path: A string representing the file path.
    :param file_format: A string representing the file format.
    :return: A DataFrame.
    """
        
    if file_format == "csv":
        data_frame = (spark
                      .read
                      .format(file_format)
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load(file_path))
    elif file_format == "json":
        data_frame = (spark
                      .read
                      .format(file_format)
                      .option("multiline", "true")
                      .load(file_path))
    else:
        raise ValueError(f"Unsupported source type: {file_format}")
    
    return data_frame

def write_data(spark, data_frame: 'DataFrame', file_path: str) -> None:
    """
    Writes data to a file path.
    :param data_frame: A DataFrame.
    :param file_path: A string representing the file path.
    :return: None.
    """                 
    (data_frame
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .mode("overwrite").save(file_path))

def rename_columns(data_frame: 'DataFrame', old_column_names: list, new_column_names: list) -> 'DataFrame':
    """
    Renames columns in a DataFrame.
    :param data_frame: A DataFrame.
    :param old_column_names: A list of strings representing the old column names.
    :param new_column_names: A list of strings representing the new column names.
    :return: A DataFrame.
    """
    for old_col, new_col in zip(old_column_names, new_column_names):
        df = df.withColumnRenamed(old_col, new_col)
    return df
