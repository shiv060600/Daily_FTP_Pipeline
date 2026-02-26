import csv, getpass

def Fixes():
    """
    Processes the IPS daily transaction file to add line numbers and unique IDs.
    
    This function:
    1. Reads the input file (IPS_DAILY_NO_LINE_NUM.TXT)
    2. Processes each order:
       - Tracks and numbers duplicate orders (e.g., if order #123 appears 3 times, they get numbers 1,2,3)
       - Generates unique IDs starting from 6300
    3. Writes the processed data to IPS_DAILY.TXT
    
    The input file is tab-delimited and contains order information.
    The output file maintains the same format but with added duplicate order numbers and IDs.
    """
    # Define input and output file paths
    ipsPath = '\\\\tutpub3\\vol2\\FOXPRO\\TestFiles\\Daily Files\\IPS_DAILY_NO_LINE_NUM.TXT'
    ipsOutPath = '\\\\tutpub3\\vol2\\FOXPRO\\TestFiles\\Daily Files\\IPS_DAILY.TXT'
    print(f"User: {getpass.getuser()}")
    try:
        # Open input and output files
        with open(ipsPath, 'r', encoding='utf-8',errors='replace') as ipscsv:
            with open(ipsOutPath, 'w', encoding='utf-8', errors='replace') as ipsoutcsv:
                # Set up CSV readers and writers with tab delimiter
                ipsreader = csv.reader(ipscsv, delimiter='\t')
                ipswriter = csv.writer(ipsoutcsv, delimiter='\t', lineterminator='\n')
                
                # Initialize tracking variables
                ordnum = ''  # Current order number
                orddict = {}  # Dictionary to track number of duplicates for each order number
                ordIddict = {}  # Dictionary to track unique IDs for each order
                id_start = 6300  # Starting point for unique IDs
                rows = []  # List to store processed rows
                
                # Process each row in the input file
                for row in ipsreader:
                    ordnum = str(row[0])  # Get order number from first column
                    
                    # Track and number duplicate orders
                    if ordnum in orddict:
                        orddict[ordnum] += 1  # Increment duplicate counter
                        row.append(orddict[ordnum])  # Add duplicate number to row
                    else:
                        orddict[ordnum] = 1  # First occurrence of this order
                        row.append(orddict[ordnum])  # Add 1 as first occurrence

                    # Add unique ID for each order
                    if ordnum in ordIddict:
                        row.append(ordIddict[ordnum])  # Use existing ID for this order
                    else:
                        ordIddict[ordnum] = id_start  # Create new ID for this order
                        row.append(ordIddict[ordnum])
                        id_start += 1  # Increment ID counter for next new order
                    
                    rows.append(row)
                
                # Write all processed rows to output file
                ipswriter.writerows(rows)
    except PermissionError as e:
        print(f"PermissionError in Fixes: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in Fixes: {e}")