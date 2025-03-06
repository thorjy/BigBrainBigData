class Compressor:
    def __init__(self):
        # Store separate dictionaries for each column's data
        self.column_dictionaries = {}  # {column_name: {value: binary_code}}
        self.column_decoders = {}      # {column_name: {binary_code: value}}
        self.column_counters = {}      # {column_name: counter for new value generation}

    def _get_column_data(self, column_name):
        """Returns the appropriate dictionaries for a given column."""
        if column_name not in self.column_dictionaries:
            self.column_dictionaries[column_name] = {}
            self.column_decoders[column_name] = {}
            self.column_counters[column_name] = 0
        return (self.column_dictionaries[column_name], self.column_decoders[column_name], self.column_counters[column_name])

    def compress(self, column_name, value):
        """Compress a value from a column into a binary representation."""
        column_data, column_decoder, column_counter = self._get_column_data(column_name)

        if value not in column_data:
            # If the value is new, assign a new binary code
            column_data[value] = bin(column_counter)[2:]  # Remove the '0b' prefix
            column_decoder[column_data[value]] = value
            column_counter += 1

        self.column_counters[column_name] = column_counter
        return column_data[value]

    def decompress(self, column_name, binary_code):
        """Decode a binary code back to the original value for a given column."""
        column_decoder = self.column_decoders.get(column_name, {})
        return column_decoder.get(binary_code, 'NULL')  # Return 'NULL' if not found
