import os
import pandas as pd

# User-defined filter times
start_time = 0  # Specify start time in minutes (e.g., 30 minutes)
end_time = 180   # Specify end time in minutes (e.g., 180 minutes)

# Directories
bridged_final_output_dir = r'C:\Users\oddon\OneDrive\SAD\BRIDGEDFINALOUTPUT'
os.makedirs(bridged_final_output_dir, exist_ok=True)
final_output_dir = r'C:\Users\oddon\OneDrive\SAD\FINALOUTPUT'

# Prepare a DataFrame to collect the counts
final_counts = pd.DataFrame()

# Group files by their base name (assumes files from the nine–16 run have '_9-16' in their name)
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
        # Identify the base file (boxes 1–8) and its nine–16 counterpart.
        base_file = next((f for f in files if '_9-16' not in f), None)
        nine_file = next((f for f in files if '_9-16' in f), None)
        if not (base_file and nine_file):
            print(f"Skipping base '{base}' because files don't match the expected pattern.")
            continue

        # Read and rename the base file columns
        base_path = os.path.join(final_output_dir, base_file)
        df_base = pd.read_csv(base_path)
        
        # Ensure the Absolute Time column is numeric
        df_base[df_base.columns[0]] = pd.to_numeric(df_base[df_base.columns[0]], errors='coerce')

        # We expect df_base to have 9 columns: the first for Absolute Time and 8 for boxes 1–8.
        if len(df_base.columns) >= 9:
            new_base_columns = [df_base.columns[0]] + [f"Box {i}" for i in range(1, 9)]
            df_base = df_base.iloc[:, :9]  # keep only the expected columns
            df_base.columns = new_base_columns
            # Remove duplicates within the base file
            df_base = df_base.drop_duplicates()

            # Filter data for the specified time range [start_time, end_time]
            df_base_filtered = df_base[(df_base[df_base.columns[0]] >= start_time) & (df_base[df_base.columns[0]] <= end_time)]
        else:
            print(f"Base file {base_file} does not have enough columns.")
            continue

        # Read and rename the nine–16 file columns
        nine_path = os.path.join(final_output_dir, nine_file)
        df_nine = pd.read_csv(nine_path)
        
        # Ensure the Absolute Time column is numeric
        df_nine[df_nine.columns[0]] = pd.to_numeric(df_nine[df_nine.columns[0]], errors='coerce')

        # We expect df_nine to have 9 columns: the first for Absolute Time and 8 for boxes 9–16.
        if len(df_nine.columns) >= 9:
            new_nine_columns = [df_nine.columns[0]] + [f"Box {i}" for i in range(9, 17)]
            df_nine = df_nine.iloc[:, :9]
            df_nine.columns = new_nine_columns
            # Remove duplicates within the nine–16 file
            df_nine = df_nine.drop_duplicates()

            # Filter data for the specified time range [start_time, end_time]
            df_nine_filtered = df_nine[(df_nine[df_nine.columns[0]] >= start_time) & (df_nine[df_nine.columns[0]] <= end_time)]
        else:
            print(f"Nine file {nine_file} does not have enough columns.")
            continue

        # Merge the two DataFrames on the "Absolute Time (minutes)" column.
        merged_df = pd.merge(df_base_filtered, df_nine_filtered, on=df_base.columns[0], how="outer")
        merged_df.sort_values(by=df_base.columns[0], ascending=True, inplace=True)
        # Remove any fully duplicated rows after sorting
        merged_df.drop_duplicates(inplace=True)

        # Count the number of 1's in each Box column (excluding 'Absolute Time' column)
        box_columns = merged_df.columns[1:]  # excluding the first column (Absolute Time)
        counts = (merged_df[box_columns] == 1).sum()

        # Prepare the results for this file
        counts_series = pd.Series(counts, name=base)

        # Add the file name as the first column
        counts_series = counts_series.to_frame().T
        counts_series.insert(0, 'File Name', base)  # Insert the file name in the first column

        # Concatenate the counts to the final counts DataFrame
        final_counts = pd.concat([final_counts, counts_series], ignore_index=True)

        # Save the bridged file
        bridged_file_path = os.path.join(bridged_final_output_dir, base + '.csv')
        merged_df.to_csv(bridged_file_path, index=False)
        print(f"Bridged file saved to {bridged_file_path}")

# After processing all files, set column names for final_counts (Box 1 to Box 16)
final_counts.columns = ['File Name'] + [f"Box {i}" for i in range(1, 17)]

# Save the final counts to a CSV file
final_counts.to_csv(os.path.join(bridged_final_output_dir, 'Final_Counts.csv'), index=False)
print(f"Final counts saved to {os.path.join(bridged_final_output_dir, 'Final_Counts.csv')}")
