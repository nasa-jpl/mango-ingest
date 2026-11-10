import ijson

def analyze_top_keys(file_path):
    max_length = 0
    longest_key = ""
    keys_printed = 0
    
    with open(file_path, 'rb') as file:
        print("### First 50 Top-Level Keys ###")
        
        for prefix, event, value in ijson.parse(file):
            
            # Look only at top-level dictionary keys
            if prefix == '' and event == 'map_key':
                
                # Print the key if we haven't reached 50 yet
                if keys_printed < 50:
                    print(f"{keys_printed + 1}: {value}")
                    keys_printed += 1
                    
                # Continue calculating the maximum length for all keys
                current_length = len(value)
                
                if current_length > max_length:
                    max_length = current_length
                    longest_key = value
                    
    print("\n" + "-" * 31)
    print(f"The longest top-level key overall is '{longest_key}' at {max_length} characters.")
    return max_length

# Run the function directly on your specific file
analyze_top_keys('offred_fields_metadata.json')
