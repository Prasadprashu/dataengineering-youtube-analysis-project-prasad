import os
import pandas as pd

def convert_to_utf8(source_dir, dest_dir):
    # Create destination directory if it doesn't exist
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    
    # Iterate through all files in the source directory
    for file_name in os.listdir(source_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(source_dir, file_name)
            print(f'Converting file: {file_path}')
            
            # Read the CSV file
            df = pd.read_csv(file_path, encoding='ISO-8859-1')
            
            # Create the new file path
            new_file_path = os.path.join(dest_dir, file_name)
            
            # Save the CSV file with UTF-8 encoding
            df.to_csv(new_file_path, encoding='utf-8', index=False)
            print(f'Saved to: {new_file_path}')

if __name__ == "__main__":
    # Define source and destination directories
    source_directory = r"C:\Users\MedabalimiBhavaniPra\Downloads\Compressed\Trending YouTube Video Statistics"
    destination_directory = r"C:\Users\MedabalimiBhavaniPra\Downloads\Compressed\Trending YouTube Video Statistics\New"
    
    # Convert all CSV files in the source directory to UTF-8 encoding
    convert_to_utf8(source_directory, destination_directory)
    print('All CSV files have been converted to UTF-8 and saved to the new folder.')


### 35:46