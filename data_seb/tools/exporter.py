import pandas as pd
from data_seb import bcra
from datetime import datetime
import os

def main():
    """Fetches major economic variables from BCRA and exports them to an Excel file."""
    print("Fetching major variables from BCRA...")
    try:
        df = bcra.get_principales_variables()
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        filename = f"bcra_variables_{timestamp}.xlsx"
        
        # In a real tool, we might want to specify an output dir
        # For now, we save in the current working directory
        df.to_excel(filename)
        
        print(f"Success! Data exported to: {os.path.abspath(filename)}")
    except Exception as e:
        print(f"Error during export: {e}")

if __name__ == "__main__":
    main()
