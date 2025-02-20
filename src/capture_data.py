import time
from pathlib import Path
from datetime import datetime
from functools import wraps
from loguru import logger
import duckdb
import yaml
import shutil

def capture_data(
    output_dir="./output",
    dbpath=":memory:",
    data_table_name="tuesday_data",
    metadata_table_name="load_metadata",
    file_info_table_name="file_info",
    image_table_name="image_data",
    max_wait_time=60  # Maximum time to wait for files in seconds
):
    def decorator(func):
        @wraps(func)
        def wrapper(asat_date: str, *args, **kwargs):
            logger.info(f"Starting function '{func.__name__}' with asat_date={asat_date}, args={args}, kwargs={kwargs}")
            
            try:
                asat_date = validate_and_parse_date(asat_date)
            except ValueError as e:
                logger.error(f"Date validation failed: {e}")
                raise

            # Capture the state of the current directory before function call
            current_dir = Path.cwd()
            files_before = set(current_dir.glob("*"))

            # Call the original function
            result = func(asat_date.strftime('%Y-%m-%d'), *args, **kwargs)

            # Wait for all files to be downloaded
            start_time = time.time()
            while True:
                files_after = set(current_dir.glob("*"))
                new_files = files_after - files_before
                
                csv_file = next((f for f in new_files if f.suffix == '.csv'), None)
                yaml_file = next((f for f in new_files if f.suffix in ['.yaml', '.yml']), None)
                image_file = next((f for f in new_files if f.suffix in ['.png', '.jpg', '.jpeg']), None)
                readme_file = next((f for f in new_files if f.name.lower() == 'readme.md'), None)
                
                if all([csv_file, yaml_file, image_file, readme_file]):
                    break
                
                if time.time() - start_time > max_wait_time:
                    error_msg = f"Timeout waiting for files. Found: {[f.name for f in new_files]}"
                    logger.error(error_msg)
                    raise TimeoutError(error_msg)
                
                time.sleep(1)  # Wait for 1 second before checking again

            # Prepare output directory
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Move new files to output directory
            required_files = [csv_file, yaml_file, image_file, readme_file]
            for file in required_files:
                dest_file = output_path / file.name
                shutil.copy2(file, dest_file)
                logger.info(f"Copied {file.name} to {dest_file}")

            conn = duckdb.connect(dbpath)

            try:
                # Load CSV data
                csv_file_path = output_path / csv_file.name
                conn.execute(f"CREATE TABLE IF NOT EXISTS {data_table_name} AS SELECT * FROM read_csv('{csv_file_path}', auto_detect=true)")
                logger.info(f"CSV data loaded into table {data_table_name}")

                # Load YAML metadata
                yaml_file_path = output_path / yaml_file.name
                with open(yaml_file_path, 'r') as f:
                    metadata = yaml.safe_load(f)
                conn.execute(f"CREATE TABLE IF NOT EXISTS {metadata_table_name} (key VARCHAR, value VARCHAR)")
                conn.execute(f"INSERT INTO {metadata_table_name} SELECT * FROM (VALUES {','.join([f'(\'{k}\', \'{v}\')' for k, v in metadata.items()])})")
                logger.info(f"YAML metadata loaded into table {metadata_table_name}")

                # Store file info
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {file_info_table_name} (
                        file_type VARCHAR,
                        file_name VARCHAR,
                        file_path VARCHAR,
                        file_content TEXT,
                        upload_date DATE
                    )
                """)

                # Store image data
                image_file_path = output_path / image_file.name
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {image_table_name} (
                        file_name VARCHAR,
                        file_path VARCHAR,
                        image_data BLOB,
                        upload_date DATE
                    )
                """)

                with open(image_file_path, 'rb') as f:
                    image_data = f.read()
                conn.execute(f"INSERT INTO {image_table_name} VALUES (?, ?, ?, ?)", 
                             [image_file.name, str(image_file_path), image_data, asat_date.date()])
                logger.info(f"Image data stored in table {image_table_name}")

                # Store image file info
                conn.execute(f"INSERT INTO {file_info_table_name} VALUES (?, ?, ?, NULL, ?)", 
                             ['image', image_file.name, str(image_file_path), asat_date.date()])

                # Store README content
                readme_file_path = output_path / readme_file.name
                with open(readme_file_path, 'r') as f:
                    readme_content = f.read()
                conn.execute(f"INSERT INTO {file_info_table_name} VALUES (?, ?, ?, ?, ?)", 
                             ['readme', readme_file.name, str(readme_file_path), readme_content, asat_date.date()])
                logger.info("README content stored in file_info table")

                # Store CSV and YAML file info
                conn.execute(f"INSERT INTO {file_info_table_name} VALUES (?, ?, ?, NULL, ?)", 
                             ['csv', csv_file.name, str(csv_file_path), asat_date.date()])
                conn.execute(f"INSERT INTO {file_info_table_name} VALUES (?, ?, ?, NULL, ?)", 
                             ['yaml', yaml_file.name, str(yaml_file_path), asat_date.date()])

                logger.info("All file information stored in file_info table")

            except Exception as e:
                logger.exception(f"Error occurred while processing files: {e}")
                raise
            finally:
                conn.close()
                logger.debug("Closed DuckDB connection")

            return result
        return wrapper
    return decorator

def validate_and_parse_date(date_str: str) -> datetime:
    """
    Validates and parses a date string. Ensures it is in YYYY-MM-DD format or raises an error.
    Returns a datetime object.
    """
    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        logger.debug(f"Parsed date successfully: {parsed_date}")
        return parsed_date
    except ValueError as e:
        logger.error(f"Invalid date format: {date_str}. Expected format is YYYY-MM-DD.")
        raise ValueError(f"Invalid date format: {date_str}. Expected format is YYYY-MM-DD.") from e
