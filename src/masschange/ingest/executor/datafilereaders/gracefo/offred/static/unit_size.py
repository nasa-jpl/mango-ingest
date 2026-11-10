import re

def get_unique_units(file_path):
    # Initialize an empty set to store our unique units
    unique_units = set()
    
    # The same regex pattern to find "UNIT": "value"
    pattern = re.compile(r'"UNIT"\s*:\s*"([^"]+)"')
    
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            match = pattern.search(line)
            
            if match:
                # Extract the unit (e.g., "DegC", "nT")
                unit_value = match.group(1)
                
                # Add it to the set. If it is already in there, 
                # the set simply ignores it!
                unique_units.add(unit_value)
                
    # Print the final list of unique units
    print(f"Found {len(unique_units)} unique units:")
    for unit in unique_units:
        print(f"- {unit}")
        
    return unique_units

# Run the function
units = get_unique_units('offred_fields_metadata.json')
print(units)
