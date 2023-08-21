import csv

# Function to read the input CSV file
def read_input_file(csv_path):
    data = []
    with open(csv_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        for row in reader:
            client_name, symbol, num_locates = row
            # Convert num_locates to an integer
            num_locates_int = int(num_locates)
            data.append((client_name, symbol, num_locates_int))
    return data

# Function to write the distributed results to an output CSV file
def write_output_file(output_csv_path, data):
    with open(output_csv_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Client', 'Symbol', 'Distributed Locates'])
        writer.writerows(data)

# Function to distribute locates to clients based on the guidelines
def distribute_locates(data):
    unique_clients_symbols = set((client, symbol) for client, symbol, _ in data)

    result = []

    for client, symbol in unique_clients_symbols:
        # Create a list of requested locates for the current client and symbol
        requested_locates = [num_locates for c, s, num_locates in data if c == client and s == symbol]
        # Calculate the total requested locates for the current client and symbol
        total_requested = sum(requested_locates)
        remaining_locates = total_requested

        while remaining_locates > 0:
            # Calculate the proportion of remaining locates relative to the total requested
            proportion = remaining_locates / total_requested
            # Calculate the number of approved locates based on the proportion
            approved_locates = int(proportion * total_requested)
            # Distribute the approved locates in chunks of 100
            approved_locates -= approved_locates % 100
            # Ensure the approved locates do not exceed the remaining locates
            approved_locates = min(approved_locates, remaining_locates)

            # Store the client, symbol, and approved locates in the result list
            result.append((client, symbol, approved_locates))
            # Decrease the remaining locates by the distributed amount
            remaining_locates -= approved_locates

    return result

if __name__ == "__main__":
    # Define input and output CSV file paths
    csv_path = r"C:\qSpark\requested_locates.csv"
    output_csv_path = r"C:\qSpark\distributed_locates.csv"
    
    try:
        # Read input data from CSV
        data = read_input_file(csv_path)
        
        # Distribute locates based on guidelines
        distributed_locates = distribute_locates(data)
        
        # Write distributed results to an output CSV file
        write_output_file(output_csv_path, distributed_locates)
        
        # Print distributed results to console
        print("\nDistributed Results:")
        for client, symbol, num_locates in distributed_locates:
            print(f"Client: {client}, Symbol: {symbol}, Distributed Locates: {num_locates}")
    except ZeroDivisionError:
        print("Error: Total requested locates is zero.")
    except Exception as e:
        print(f"An error occurred: {e}")
