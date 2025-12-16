# cli.py
from sql_engine import SimpleSQLEngine

def format_results(results: list):
    """Prints the list of dictionaries as a neatly formatted table."""
    if not results:
        print("  (0 rows returned)")
        return

    # Get all column names (keys) from all rows
    all_keys = set()
    for row in results:
        all_keys.update(row.keys())
    
    columns = list(all_keys)
    
    # Calculate column widths
    col_widths = {col: len(col) for col in columns}
    for row in results:
        for col in columns:
            val_str = str(row.get(col, ''))
            col_widths[col] = max(col_widths[col], len(val_str))

    # --- Print Header ---
    header_line = ""
    separator_line = ""
    for col in columns:
        width = col_widths[col]
        header_line += f"| {col.upper():<{width}} "
        separator_line += f"+{'-' * (width + 2)}"
        
    print(separator_line + "+")
    print(header_line + "|")
    print(separator_line + "+")

    # --- Print Rows ---
    for row in results:
        row_line = ""
        for col in columns:
            width = col_widths[col]
            value = row.get(col, '')
            val_str = str(value)
            
            if isinstance(value, (int, float)):
                 row_line += f"| {val_str:>{width}} " # Right-align numbers
            else:
                 row_line += f"| {val_str:<{width}} " # Left-align everything else
                 
        print(row_line + "|")
        
    print(separator_line + "+")
    print(f"({len(results)} rows returned)")


def main():
    engine = SimpleSQLEngine()
    
    print("Welcome to SimpleSQL Engine.")
    print("Commands: LOAD <filename> AS <table_name>, SELECT ... FROM ...")
    print("Type 'exit' or 'quit' to close the engine.")
    
    while True:
        try:
            user_input = input("\nSQL-Engine> ").strip()
            
            if not user_input:
                continue
                
            command = user_input.upper().split(None, 1)[0]
            
            if command in ('EXIT', 'QUIT'):
                print("Exiting SimpleSQL Engine. Goodbye!")
                break
                
            elif command == 'LOAD':
                parts = user_input.split()
                if len(parts) != 4 or parts[2].upper() != 'AS':
                    print("Error: Invalid LOAD command syntax. Use: LOAD <filename> AS <table_name>")
                    continue
                
                filename = parts[1]
                table_name = parts[3]
                engine.load_csv(filename, table_name)
                
            elif command == 'SELECT':
                results = engine.execute_query(user_input)
                format_results(results)

            else:
                print(f"Error: Unsupported command '{command}'.")

        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()