import os

class ColumnStore:
    def __init__(self, logger):
        self.logger = logger

    def create_column_stores(self, directory, df):
        """Creates a file for each key column and stores data inside it"""
        self.logger.info(f"Creating column store files in {directory}...")

        if not os.path.exists(directory):
            self.logger.info(f"Directory {directory} does not exist. Creating it...")
            os.makedirs(directory)

        for col in df.columns:
            self.logger.info(f"Processing column: {col}...")

            # Write the processed column data to a file
            file_path = os.path.join(directory, f"{col}.txt")
            with open(file_path, "w", encoding="utf-8") as f:
                for value in df[col]:
                    f.write(str(value) + "\n")
            self.logger.info(f"Column {col} saved to {file_path}.")

        self.logger.info("All column store files have been created.")
