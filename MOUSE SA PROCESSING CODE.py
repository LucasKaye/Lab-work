import csv
import pandas as pd
import os

# =============================================================================
# CHANGE: Wrapped all processing steps in a function so it can be called twice.
# =============================================================================
def run_raster_plot_parsing(boxNumbers=''):
    """
    Processes files from the base input folder (or its subfolder) by:
      - Converting to TXT (tab-delimited) format.
      - Parsing and aligning absolute time data from "C:" sections.
      - Creating a CSV file with aligned time and then processing that CSV for
        raster readiness.
      - Extracting specific Excel columns.
      
    The 'boxNumbers' parameter (if provided) is appended as a suffix to output files
    and used as a subfolder name in the input directory.
    """

    # =============================================================================
    # CHANGE: Set base input directory and use 'boxNumbers' to select a subfolder if given.
    # =============================================================================
    base_input_dir = r'C:\Users\oddon\OneDrive\SAD\files'
    if boxNumbers:
        # Using os.path.join to combine the base directory and subfolder.
       input_directory = base_input_dir + boxNumbers
    else:
        input_directory = base_input_dir

    # =============================================================================
    # CHANGE: Define a suffix to append to output file names when boxNumbers is provided.
    # =============================================================================
    suffix = "_" + boxNumbers if boxNumbers else ""

    # Directories remain constant.
    output_directory = r'C:\Users\oddon\OneDrive\SAD\SA Data to process'
    raster_ready_dir = r'C:\Users\oddon\OneDrive\SAD\Raster ready'
    final_output_dir = r'C:\Users\oddon\OneDrive\SAD\FINALOUTPUT'

    # Ensure necessary directories exist.
    os.makedirs(output_directory, exist_ok=True)
    os.makedirs(raster_ready_dir, exist_ok=True)
    os.makedirs(final_output_dir, exist_ok=True)

    # =============================================================================
    # CHANGE: Use the original conversion function to convert input files.
    # =============================================================================
    def convert_to_txt(input_file, output_file):
        try:
            if input_file.endswith('.csv'):
                df = pd.read_csv(input_file)
                df.to_csv(output_file, sep='\t', index=False)
            elif input_file.endswith('.xlsx') or input_file.endswith('.xls'):
                df = pd.read_excel(input_file)
                df.to_csv(output_file, sep='\t', index=False)
            elif input_file.endswith('.json'):
                df = pd.read_json(input_file)
                df.to_csv(output_file, sep='\t', index=False)
            else:
                with open(input_file, 'r', encoding='utf-8', errors='ignore') as file:
                    content = file.read()
                with open(output_file, 'w', encoding='utf-8') as out_file:
                    out_file.write(content)
            print(f"Data from {input_file} has been successfully written to {output_file}")
        except Exception as e:
            print(f"Failed to convert {input_file} to {output_file}. Error: {e}")

    # =============================================================================
    # Loop through all files in the input directory.
    # =============================================================================
    for file_name in os.listdir(input_directory):
        input_file = os.path.join(input_directory, file_name)
        base_name = os.path.splitext(file_name)[0]
        # =============================================================================
        # CHANGE: Append the suffix to the output file name.
        # =============================================================================
        output_file_txt = os.path.join(output_directory, base_name + suffix + '.txt')

        if os.path.isfile(input_file):
            convert_to_txt(input_file, output_file_txt)
        else:
            print(f"Skipping directory: {file_name}")
            continue

        # =============================================================================
        # Initialize storage for each box (8 boxes).
        # =============================================================================
        box_data = [{} for _ in range(8)]  # Each box: {absolute_time: (raw, fraction)}

        # =============================================================================
        # CHANGE: Define process_time function as in the first file.
        # =============================================================================
        def process_time(time_string):
            """Extracts time value and converts it to absolute minutes, only keeping specific fractions."""
            try:
                integer_part, fractional_part = time_string.split(".")
                integer_part = int(integer_part)
                fractional_part = int(fractional_part) if fractional_part else 0
                fractional_part = fractional_part // 100  # Keep only first two digits

                if fractional_part in (1, 2, 6):
                    time_in_minutes = (integer_part * 0.01) / 60.0  # Convert to minutes
                    return time_in_minutes, fractional_part
                else:
                    return None, None
            except ValueError:
                return None, None

        # =============================================================================
        # CHANGE: Read the processed TXT file (the one we just generated) to parse aligned data.
        # =============================================================================
        processed_input_file = os.path.join(output_directory, base_name + suffix + '.txt')
        with open(processed_input_file, 'r') as file:
            in_c_section = False
            current_box_index = -1  # will increment when a line starts with "C:"
            last_absolute_time = [0] * 8  # Track cumulative time per box

            for line in file:
                if line.strip().startswith('C:'):
                    current_box_index += 1
                    if current_box_index >= 8:
                        print("Warning: More than 8 boxes detected, ignoring extras.")
                        break
                    in_c_section = True
                    continue

                if in_c_section:
                    # End C-section if line is blank or starts with any of these labels.
                    if line.strip() == '' or line.strip().startswith(('A:', 'E:', 'F:', 'I:', 'L:', 'R:', 'S:', 'T:', 'V:', 'J:', 'W:')):
                        in_c_section = False
                        continue

                    parts = line.strip().split()
                    numbers = parts[1:]  # Skip the index
                    for num in numbers:
                        time_in_minutes, fractional_part = process_time(num)
                        if time_in_minutes is not None:
                            last_absolute_time[current_box_index] += time_in_minutes
                            box_data[current_box_index][last_absolute_time[current_box_index]] = (num, fractional_part)

        # =============================================================================
        # Create a global absolute time axis and prepare header rows.
        # =============================================================================
        all_absolute_times = sorted(set(time for box in box_data for time in box.keys()))
        header = ['Raw Data', 'Fraction']
        # Header order: Boxes 1-4 then Boxes 5-8.
        box_headers = [f'Box {i+1} {x}' for i in range(4) for x in header] + \
                      [f'Box {i+5} {x}' for i in range(4) for x in header]
        final_headers = ['Absolute Time (minutes)'] + box_headers

        # =============================================================================
        # CHANGE: Overwrite the TXT file with a CSV file containing the aligned data.
        # =============================================================================
        with open(output_file_txt, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(final_headers)
            for absolute_time in all_absolute_times:
                row = [absolute_time]
                for box in box_data:
                    if absolute_time in box:
                        raw_value, fraction_value = box[absolute_time]
                        row.append(raw_value)
                        row.append(fraction_value)
                    else:
                        row.extend(['', ''])
                writer.writerow(row)
        print(f"Aligned absolute time data successfully written to {output_file_txt}")

        # =============================================================================
        # Process the aligned CSV for raster readiness.
        # =============================================================================
        df = pd.read_csv(output_file_txt)

        # =============================================================================
        # CHANGE: Remove all "Raw Data" columns.
        # =============================================================================
        df = df.drop(columns=[col for col in df.columns if "Raw Data" in col])

        # =============================================================================
        # CHANGE: Create new columns for each box and fraction combination.
        # =============================================================================
        boxes = range(1, 9)  # 8 boxes
        fraction_values = [1, 2, 6]
        for box in boxes:
            for value in fraction_values:
                df[f"Box {box}-{value}"] = None

        # =============================================================================
        # CHANGE: Populate the new columns based on the Fraction values.
        # =============================================================================
        for box in boxes:
            fraction_col = f"Box {box} Fraction"
            for index, row in df.iterrows():
                # Use .get() to safely access the column if it exists.
                if not pd.isna(row.get(fraction_col)) and row[fraction_col] in fraction_values:
                    df.at[index, f"Box {box}-{int(row[fraction_col])}"] = 1
            # Extra step if Fraction equals 2.
            for index, row in df.iterrows():
                if not pd.isna(row.get(fraction_col)) and row[fraction_col] == 2:
                    if row.get(f"Box {box}-2") == 1:
                        df.at[index, f"Box {box}-1"] = 1

        # =============================================================================
        # CHANGE: Drop the original "Fraction" columns that are not part of the new ones.
        # =============================================================================
        df = df.drop(columns=[col for col in df.columns if "Fraction" in col and "Box" not in col], errors='ignore')

        # Save the processed data to the Raster ready folder.
        raster_output_file = os.path.join(raster_ready_dir, base_name + suffix + '.csv')
        df.to_csv(raster_output_file, index=False)
        print(f"Processed data saved to {raster_output_file}")

    # =============================================================================
    # CHANGE: Now process each CSV file in the raster ready directory to keep only specific Excel columns.
    # =============================================================================
    excel_columns = ['A', 'J', 'M', 'P', 'S', 'V', 'Y', 'AB', 'AE']
    def excel_col_to_index(col):
        index = 0
        for c in col:
            index = index * 26 + (ord(c.upper()) - ord('A') + 1)
        return index - 1

    indices = [excel_col_to_index(col) for col in excel_columns]

    for file_name in os.listdir(raster_ready_dir):
        if file_name.lower().endswith('.csv'):
            file_path = os.path.join(raster_ready_dir, file_name)
            df = pd.read_csv(file_path)
            valid_indices = [i for i in indices if i < len(df.columns)]
            df_subset = df.iloc[:, valid_indices]
            final_file_path = os.path.join(final_output_dir, file_name)
            df_subset.to_csv(final_file_path, index=False)
            print(f"Final output saved to {final_file_path}")

# =============================================================================
# CHANGE: Call the parsing function twice with different boxNumbers (subfolders).
# =============================================================================
run_raster_plot_parsing(boxNumbers='')       # Process files from the base folder.
run_raster_plot_parsing(boxNumbers='9-16')     # Process files from the "9-16" subfolder.

# =============================================================================
# CHANGE: After both runs, combine files with matching base names into a bridged final output.
# =============================================================================
bridged_final_output_dir = r'C:\Users\oddon\OneDrive\SAD\BRIDGEDFINALOUTPUT'
os.makedirs(bridged_final_output_dir, exist_ok=True)
final_output_dir = r'C:\Users\oddon\OneDrive\SAD\FINALOUTPUT'

# Group files by their base name (assumes files from the second run have an underscore in their name)
files_by_base = {}
for file_name in os.listdir(final_output_dir):
    if file_name.lower().endswith('.csv'):
        if '_9-16' in file_name:
            base = file_name.replace('_9-16', '')
            base = os.path.splitext(base)[0]
        else:
            base = os.path.splitext(file_name)[0]
        files_by_base.setdefault(base, []).append(file_name)

for base, files in files_by_base.items():
    if len(files) == 2:
        # Verify we have a base file and its _9-16 counterpart
        base_file = next((f for f in files if '_9-16' not in f), None)
        nine_sixteen_file = next((f for f in files if '_9-16' in f), None)
        
        if not (base_file and nine_sixteen_file):
            print(f"Skipping base '{base}' because files don't match the expected pattern.")
            continue
            
        # Process base file (boxes 1-8)
        path = os.path.join(final_output_dir, base_file)
        df_base = pd.read_csv(path)
        
        # Process 9-16 file
        path = os.path.join(final_output_dir, nine_sixteen_file)
        df_nine = pd.read_csv(path)
        
        # Rename columns in the 9-16 file to Box 9-16
        new_columns = [df_nine.columns[0]]  # Keep the Absolute time column name
        for i, col in enumerate(df_nine.columns[1:], start=9):
            new_columns.append(f'Box {i}')
        df_nine.columns = new_columns
        
        # Create empty columns for boxes 1-8 in the 9-16 dataframe
        empty_cols = pd.DataFrame('', index=df_nine.index, 
                                columns=[f'Box {i}' for i in range(1, 9)])
        
        # Combine the pieces for the 9-16 dataframe
        df_nine = pd.concat([
            df_nine[df_nine.columns[0]],  # Absolute time
            df_nine[df_nine.columns[1:]]   # Box 9-16 data
        ], axis=1)
        
        # Combine both dataframes and sort by Absolute time
        combined_df = pd.concat([df_base, df_nine], ignore_index=True)
        combined_df.sort_values(by=combined_df.columns[0], ascending=True, inplace=True)
        
        bridged_file_path = os.path.join(bridged_final_output_dir, base + '.csv')
        combined_df.to_csv(bridged_file_path, index=False)
        print(f"Bridged file saved to {bridged_file_path}")
    else:
        print(f"Skipping base '{base}' because it does not have exactly 2 matching files.")
