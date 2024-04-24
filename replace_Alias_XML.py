import os
import zipfile
import shutil
import xml.etree.ElementTree as ET

# Define the directory paths
zip_directory = 'path/to/zip/files'
extract_directory = 'path/to/extract/files'

# Define the find and replace mappings
find_replace_mappings = {
    'old_text_1': 'new_text_1',
    'old_text_2': 'new_text_2',
    # Add more mappings as needed
}

# Unzip the files
for file_name in os.listdir(zip_directory):
    if file_name.endswith('.zip'):
        zip_path = os.path.join(zip_directory, file_name)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_directory)

# Process the extracted XML files
for file_name in os.listdir(extract_directory):
    if file_name.endswith('.xml'):
        xml_path = os.path.join(extract_directory, file_name)
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Find and replace text within the XML file
        for elem in root.iter():
            if elem.text is not None:
                for find_text, replace_text in find_replace_mappings.items():
                    elem.text = elem.text.replace(find_text, replace_text)

        # Overwrite the original XML file with the modified file
        tree.write(xml_path)

# Re-zip the modified files
output_zip_path = 'path/to/output/modified.zip'
with zipfile.ZipFile(output_zip_path, 'w') as zip_ref:
    for file_name in os.listdir(extract_directory):
        file_path = os.path.join(extract_directory, file_name)
        zip_ref.write(file_path, file_name)

# Clean up the extracted files
shutil.rmtree(extract_directory)

print("XML files modified and re-zipped successfully!")
