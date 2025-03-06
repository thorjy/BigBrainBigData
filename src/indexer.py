class Indexer:
    def __init__(self):
        # Index map stores the start and end indexes for each unique value in a column
        # {col_name: {value: [{start, end}]}}
        self.index_map = {}
        self.index_count = 0  # Keep track of the number of rows indexed

    def create_index(self, df, col_name):
        """
        Creates an index for the specified column, storing the start and end indices
        for each unique value in the column.
        """
        col_data = df[col_name]  # Extract the column data

        # If the column is not already in the index map, add it
        if col_name not in self.index_map:
            self.index_map[col_name] = {}

        prev_value = None  # To track the previous value to detect changes

        # Iterate over the column data
        for idx, value in enumerate(col_data):
            # If the value has changed (or it's the first iteration), close the previous range and start a new one
            if value != prev_value:
                if prev_value is not None:  # If there's a previous value, finalize its range
                    self.index_map[col_name][prev_value][-1]['end'] = idx - 1

                # Start a new range for the current value
                if value not in self.index_map[col_name]:
                    self.index_map[col_name][value] = []

                self.index_map[col_name][value].append({'start': idx, 'end': idx})  # New range starts

            # Update the previous value to the current one
            prev_value = value
            self.index_count += 1  # Increment row count

        # Finalize the last range
        if prev_value is not None:
            self.index_map[col_name][prev_value][-1]['end'] = len(col_data) - 1

        print(f"Indexing completed. Indexed {self.index_count} unique values in column '{col_name}'.")

    def get_index(self, col_name, value):
        """
        Retrieve the list of start and end ranges for a given value in the specified column.
        """
        return self.index_map.get(col_name, {}).get(value, [])

    def get_all_indexes(self):
        """Retrieve all the indexes (start, end ranges) for all values in all columns."""
        return self.index_map
