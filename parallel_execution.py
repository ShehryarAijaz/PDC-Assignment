import pandas as pd
import multiprocessing as mp
import time

# File paths
students_file_path = "Datasets/students.csv"
fees_file_path = "Datasets/student_fees.csv"

# Load datasets
students_df = pd.read_csv(students_file_path)
fees_df = pd.read_csv(fees_file_path)

# Extract the day from the Payment Date in the fees dataset
fees_df['Day'] = fees_df['Payment Date'].str.extract(r'(\d+)$').astype(int)

# Function to determine the most frequent payment day for a dataset chunk
def calculate_payment_pattern(chunk):
    # Dictionary to track payment days frequencies
    payment_day_count = {}

    for _, row in chunk.iterrows():
        student_id = row['Student ID']
        payment_day = row['Day']
        if student_id not in payment_day_count:
            payment_day_count[student_id] = {}
        payment_day_count[student_id].setdefault(payment_day, 0)
        payment_day_count[student_id][payment_day] += 1

    # Find the most common payment day for each student in the chunk
    most_common_payment_day = {}
    for student_id, day_count in payment_day_count.items():
        most_common_payment_day[student_id] = max(day_count, key=day_count.get)

    return most_common_payment_day

if __name__ == '__main__':
    # Print the starting message
    print("Starting Parallel Processing...")

    # Record the start time
    start_time = time.time()

    # Split the fees data into chunks for parallel processing
    num_partitions = mp.cpu_count()
    chunk_size = len(fees_df) // num_partitions
    chunks = [fees_df.iloc[i:i + chunk_size] for i in range(0, len(fees_df), chunk_size)]

    # Use multiprocessing to handle each chunk in parallel
    with mp.Pool(num_partitions) as pool:
        results = pool.map(calculate_payment_pattern, chunks)

    # Merge all results
    consistent_payment_days_dict = {}
    for result in results:
        consistent_payment_days_dict.update(result)

    # Convert the dictionary to a DataFrame
    consistent_payment_days = pd.DataFrame(list(consistent_payment_days_dict.items()), columns=['Student ID', 'Most Consistent Payment Day'])

    # Combine the payment data with student details
    merged_df = pd.merge(students_df, consistent_payment_days, on='Student ID', how='inner')

    # Record the end time
    end_time = time.time()
    execution_time = end_time - start_time

    # Print execution time and a preview of the merged DataFrame
    print(f"Execution Time: {execution_time:.4f} seconds")
    print(merged_df.head())

    # Output the merged DataFrame to a CSV file
    output_file_path = "parallel_output.csv"
    merged_df.to_csv(output_file_path, index=False)

    print(f"Merged DataFrame saved to: {output_file_path}")
