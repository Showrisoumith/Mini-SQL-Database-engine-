import csv
import re 

class SimpleSQLEngine:
    """
    An in-memory, simplified SQL query engine built from scratch.
    It handles data loading, parsing, filtering (WHERE), projection (SELECT),
    and aggregation (COUNT). Table and column lookups are case-insensitive.
    """
    def __init__(self):
        # self.table_data stores: { 'table_name': { 'columns': [...], 'rows': [{...}, {...}] } }
        self.table_data = {}

    def load_csv(self, filename: str, table_name: str):
        """
        Loads data from a CSV file into an in-memory table.
        Table and Column names are normalized to lowercase for storage and lookup.
        """
        if not filename.lower().endswith('.csv'):
             raise Exception(f"Error: File '{filename}' must be a CSV file.")
             
        # 1. Normalize table name to lowercase
        normalized_table_name = table_name.lower()
        
        try:
            with open(filename, 'r', newline='') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                if not rows:
                    raise Exception(f"Error: CSV file '{filename}' is empty or has no data rows.")
                
                # 2. Normalize column headers (keys) to lowercase
                columns = [col.lower() for col in reader.fieldnames]

                # 3. Normalize row keys to lowercase and prepare for type casting/cleaning
                normalized_rows = [{k.lower(): v for k, v in row.items()} for row in rows]
                
                self.table_data[normalized_table_name] = {
                    'columns': columns, 
                    'rows': self._cast_data_types(normalized_rows)
                }
            print(f"✅ Loaded '{filename}' as table '{normalized_table_name}' with {len(rows)} rows.")
            
        except FileNotFoundError:
            raise Exception(f"Error: File not found: {filename}")
        except Exception as e:
            raise Exception(f"Error loading CSV: {e}")

    def _cast_data_types(self, rows: list) -> list:
        """
        Helper to try and convert string data to numbers and strip whitespace
        from remaining strings for accurate comparison.
        """
        if not rows:
            return []
        
        sample_rows = rows[:min(10, len(rows))]
        numeric_cols = {}
        
        # Identify numeric columns
        for col in rows[0].keys():
            is_int = True
            is_float = True
            for row in sample_rows:
                value = row.get(col, '')
                if value == '': 
                    continue
                try:
                    int(value)
                except ValueError:
                    is_int = False
                try:
                    float(value)
                except ValueError:
                    is_float = False
            
            if is_int and not is_float:
                 numeric_cols[col] = int
            elif is_float:
                numeric_cols[col] = float
        
        # Apply type casting and string cleaning (stripping whitespace)
        casted_rows = []
        for row in rows:
            new_row = {}
            for col, value in row.items():
                if col in numeric_cols and value != '':
                    new_row[col] = numeric_cols[col](value)
                else:
                    # Strip whitespace from string values
                    new_row[col] = value.strip() if isinstance(value, str) else value 
                    
            casted_rows.append(new_row)
            
        return casted_rows

    def parse_query(self, sql_query: str) -> dict:
        """
        Parses a simplified SQL query string into a structured dictionary.
        Normalizes table and column names to lowercase.
        """
        sql_query = ' '.join(sql_query.strip().split())
        upper_query = sql_query.upper()
        
        # 1. Extract FROM and Table Name
        if 'FROM' not in upper_query:
            raise Exception("Syntax Error: Missing FROM clause.")

        from_parts = upper_query.split('FROM', 1)
        select_clause = from_parts[0].replace('SELECT', '', 1).strip()
        
        remaining_query = from_parts[1].strip()
        
        if 'WHERE' in remaining_query:
            table_name_raw = remaining_query.split('WHERE', 1)[0].strip()
            where_clause_str = remaining_query.split('WHERE', 1)[1].strip()
        else:
            table_name_raw = remaining_query.strip()
            where_clause_str = None
        
        if not table_name_raw or table_name_raw.split()[0].upper() in ('WHERE', 'GROUP', 'ORDER'):
            raise Exception("Syntax Error: Missing or invalid table name after FROM.")
        
        table_name = table_name_raw.split()[0].lower()
        
        if table_name not in self.table_data:
            raise Exception(f"Query Error: Table '{table_name}' has not been loaded.")
            
        parsed_query = {
            'table': table_name,
            'select_cols': None,
            'where': None,
            'is_aggregate': False
        }

        # 2. Extract SELECT Columns/Function
        if not select_clause:
            raise Exception("Syntax Error: Missing columns/function in SELECT clause.")
        
        if select_clause.startswith('COUNT(') and select_clause.endswith(')'):
            col_or_star = select_clause[6:-1].strip().lower() 
            parsed_query['is_aggregate'] = True
            parsed_query['select_cols'] = {'func': 'COUNT', 'target': col_or_star}
        else:
            # Normalize selected columns to lowercase
            parsed_query['select_cols'] = [c.strip().lower() for c in select_clause.split(',')]
            
        # 3. Extract WHERE clause (The originally incomplete part)
        if where_clause_str:
            match = re.search(r"(\w+)\s*(>=|<=|!=|=|>|<)\s*([\"']?[^\s\"']+[^\)]*[\"']?)$", where_clause_str, re.IGNORECASE)
            
            if not match:
                raise Exception("Syntax Error: Invalid WHERE clause format.")

            col, op, raw_val = match.groups()
            
            # Normalize WHERE column name to lowercase
            col = col.lower() 
            
            # Clean up the value (remove surrounding quotes)
            value = raw_val.strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                value = value[1:-1]
            
            # Convert value to number if possible
            try:
                if re.match(r'^\d+$', value):
                    value = int(value)
                elif re.match(r'^\d+\.\d+$', value):
                    value = float(value)
            except ValueError:
                pass 

            parsed_query['where'] = {
                'column': col, 
                'operator': op,
                'value': value
            }

            if col not in self.table_data[table_name]['columns']:
                raise Exception(f"Query Error: Column '{col}' in WHERE clause does not exist.")

        return parsed_query # The function must return the parsed dictionary!

    def _evaluate_condition(self, row: dict, where_clause: dict) -> bool:
        """Evaluates a single WHERE condition against a single row, 
        using case-insensitive comparison for strings (equality ops).
        """
        col = where_clause['column']
        op = where_clause['operator']
        target_val = where_clause['value']

        row_val = row.get(col)
        
        # Handle null/empty values
        if row_val is None or row_val == '':
            if op == '=':
                return target_val is None or target_val == ''
            elif op == '!=':
                return target_val is not None and target_val != ''
            return False 

        # Handle type mismatch
        try:
            if isinstance(target_val, (int, float)):
                row_val = type(target_val)(row_val) 
            elif isinstance(row_val, (int, float)) and isinstance(target_val, str):
                 target_val = type(row_val)(target_val)
        except (TypeError, ValueError):
            pass 

        # Perform the comparison (using .lower() for string equality)
        if op == '=':
            if isinstance(row_val, str) and isinstance(target_val, str):
                return row_val.lower() == target_val.lower()
            return row_val == target_val
        elif op == '!=':
            if isinstance(row_val, str) and isinstance(target_val, str):
                return row_val.lower() != target_val.lower()
            return row_val != target_val
        elif op == '>':
            return row_val > target_val
        elif op == '<':
            return row_val < target_val
        elif op == '>=':
            return row_val >= target_val
        elif op == '<=':
            return row_val <= target_val
        else:
            raise Exception(f"Unsupported operator: {op}")


    def _apply_filtering(self, rows: list, where_clause: dict) -> list:
        """Applies the WHERE clause to the list of rows."""
        if not where_clause:
            return rows
        
        filtered_rows = []
        for row in rows:
            if self._evaluate_condition(row, where_clause):
                filtered_rows.append(row)
        
        return filtered_rows
    
    def execute_query(self, sql_query: str):
        """
        Main method to parse and execute a SQL query.
        Returns a list of dictionaries (result rows).
        """
        parsed_q = self.parse_query(sql_query)
        table_name = parsed_q['table'] 
        table_data = self.table_data[table_name]
        
        # 1. Start with all rows from the table
        current_rows = table_data['rows']
        
        # 2. Apply Filtering (WHERE)
        current_rows = self._apply_filtering(current_rows, parsed_q['where'])
        
        # 3. Handle Aggregation (COUNT)
        if parsed_q['is_aggregate']:
            func_details = parsed_q['select_cols']
            func = func_details['func']
            target = func_details['target']
            
            if func == 'COUNT':
                if target == '*':
                    count_result = len(current_rows)
                else:
                    if target not in table_data['columns']:
                        raise Exception(f"Query Error: Column '{target}' in COUNT() does not exist.")
                        
                    count_result = sum(1 for row in current_rows if row.get(target) is not None and row.get(target) != '')
                    
                return [{'COUNT': count_result}]

        # 4. Handle Projection (SELECT)
        select_cols = parsed_q['select_cols']
        result_rows = []
        
        if select_cols == ['*']:
            result_rows = current_rows
        else:
            for col in select_cols:
                if col not in table_data['columns']:
                    raise Exception(f"Query Error: Column '{col}' in SELECT clause does not exist.")

            for row in current_rows:
                # Create a new dictionary containing only the selected columns
                new_row = {col: row.get(col) for col in select_cols}
                result_rows.append(new_row)
        
        return result_rows