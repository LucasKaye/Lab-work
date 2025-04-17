import csv
import pandas as pd
import os

def run_raster_plot_parsing():
    """
    Processes a single data file (with up to 16 boxes in "C:" sections) by:
      1. Converting the file to a tab-delimited TXT.
      2. Parsing only fraction=1 events (ignoring fraction=2 or 6).
      3. Creating a single CSV with columns:
         [Absolute Time (minutes), Box 1, Box 2, ..., Box 16].
      4. Each box column has '1' if fraction=1 occurred, otherwise blank.
    """

    # -- Folders (adjust as needed) --------------------------------------------
    base_input_dir = r'C:\Users\oddon\OneDrive\SAD\RATSA RAW'         # Where the raw files are
    output_directory = r'C:\Users\oddon\OneDrive\SAD\SA Data to process'
    final_output_dir = r'C:\Users\oddon\OneDrive\SAD\RATSA FINAL'
    
    # Ensure output directories exist
    os.makedirs(output_directory, exist_ok=True)
    os.makedirs(final_output_dir, exist_ok=True)

    # -- Helper function: Convert various file types to TXT -------------------
    def convert_to_txt(input_file, output_file):
        try:
            if input_file.endswith('.csv'):
                df = pd.read_csv(input_file)
                df.to_csv(output_file, sep='\t', index=False)
            elif input_file.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(input_file)
                df.to_csv(output_file, sep='\t', index=False)
            elif input_file.endswith('.json'):
                df = pd.read_json(input_file)
                df.to_csv(output_file, sep='\t', index=False)
            else:
                # Generic text file; just copy contents
                with open(input_file, 'r', encoding='utf-8', errors='ignore') as file:
                    content = file.read()
                with open(output_file, 'w', encoding='utf-8') as out_file:
                    out_file.write(content)
            print(f"Data from {input_file} has been successfully written to {output_file}")
        except Exception as e:
            print(f"Failed to convert {input_file} to {output_file}. Error: {e}")

    # -- Helper function: Convert only fraction=1 to minutes ------------------
    def process_time(time_string):
        """
        Extracts time value and converts it to minutes if fraction=1.
        Ignores fraction=2 or fraction=6 (returns None, None).
        """
        try:
            integer_part, fractional_part = time_string.split(".")
            integer_part = int(integer_part)
            fractional_part = int(fractional_part) if fractional_part else 0
            fractional_part = fractional_part // 100  # Keep only first two digits

            # Only process fraction=1; ignore fraction=2 or 6
            if fractional_part == 1:
                time_in_minutes = (integer_part * 0.01) / 60.0
                return time_in_minutes, fractional_part
            else:
                return None, None
        except ValueError:
            return None, None

    # -- Main loop: Convert and process each file in the input directory -------
    for file_name in os.listdir(base_input_dir):
        input_file = os.path.join(base_input_dir, file_name)
        
        if not os.path.isfile(input_file):
            print(f"Skipping directory: {file_name}")
            continue
        
        base_name = os.path.splitext(file_name)[0]
        output_file_txt = os.path.join(output_directory, base_name + '.txt')
        
        # 1) Convert input to TXT (tab-delimited)
        convert_to_txt(input_file, output_file_txt)
        
        # 2) Parse the TXT for up to 16 boxes, but only record fraction=1 events
        box_data = [{} for _ in range(16)]  # each index -> {time: 1} if fraction=1
        last_absolute_time = [0.0] * 16
        
        current_box_index = -1
        in_c_section = False
        
        with open(output_file_txt, 'r') as f:
            for line in f:
                line_stripped = line.strip()
                
                # When we see "C:", move to next box
                if line_stripped.startswith('C:'):
                    current_box_index += 1
                    if current_box_index >= 16:
                        print("Warning: More than 16 boxes detected; ignoring extras.")
                        break
                    in_c_section = True
                    continue
                
                # If we are in a "C:" section but reach a blank or new label, exit C-section
                if in_c_section and (
                    line_stripped == '' or
                    line_stripped.startswith(('A:', 'E:', 'F:', 'I:', 'L:', 'R:', 'S:', 'T:', 'V:', 'J:', 'W:'))
                ):
                    in_c_section = False
                    continue
                
                # If still in the C-section, parse times (only fraction=1)
                if in_c_section:
                    parts = line_stripped.split()
                    # The first part is typically an index; skip it
                    numbers = parts[1:]
                    for num in numbers:
                        t_minutes, frac = process_time(num)
                        if t_minutes is not None and frac == 1:
                            # Accumulate time for the current box
                            last_absolute_time[current_box_index] += t_minutes
                            # Store a marker at that absolute time
                            box_data[current_box_index][last_absolute_time[current_box_index]] = 1
        
        # 3) Create a single CSV: "Absolute Time (minutes), Box 1, ..., Box 16"
        #    Only times for fraction=1 will appear.
        all_times = sorted(set(t for box in box_data for t in box.keys()))
        final_headers = ['Absolute Time (minutes)'] + [f'Box {i+1}' for i in range(16)]
        
        # Write the aligned data to a CSV
        aligned_csv_path = os.path.join(output_directory, base_name + '_aligned.csv')
        with open(aligned_csv_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(final_headers)
            
            for t in all_times:
                row = [t]
                for i in range(16):
                    # Put a '1' if we have an event, else blank
                    val = box_data[i].get(t, '')
                    row.append(val)
                writer.writerow(row)
        
        print(f"Aligned data for fraction=1 (16 boxes) written to {aligned_csv_path}")
        
        # 4) (Optional) If you want a final copy in FINALOUTPUT, do so here
        final_file_path = os.path.join(final_output_dir, base_name + '_final.csv')
        df_aligned = pd.read_csv(aligned_csv_path)
        df_aligned.to_csv(final_file_path, index=False)
        
        print(f"Final output saved to {final_file_path}")


# ---------------------------------------------------------------------------
# Run the function (process all files in the folder).
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_raster_plot_parsing()
