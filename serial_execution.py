import pandas as pd
from collections import Counter
import time

# File paths
students_file_path = "Datasets/students.csv"
fees_file_path = "Datasets/student_fees.csv"

# Serial Processing
print("Starting Serial Processing...")
start_time = time.time()

# Load datasets
students_df = pd.read_csv(students_file_path)
fees_df = pd.read_csv(fees_file_path)

# Handle missing values before extracting and converting to integer
fees_df['Day'] = fees_df['Payment Date'].str.extract(r'(\d+)$')
fees_df['Day'] = pd.to_numeric(fees_df['Day'], errors='coerce').fillna(0).astype(int)

# Calculate the most frequent payment day for each student
payment_day_count = {}
for _, row in fees_df.iterrows():
    student_id = row['Student ID']
    payment_day = row['Day']
    if student_id not in payment_day_count:
        payment_day_count[student_id] = Counter()
    payment_day_count[student_id][payment_day] += 1

# Find the most consistent payment day
most_consistent_days = {
    student_id: max(day_count, key=day_count.get)
    for student_id, day_count in payment_day_count.items()
}

# Convert to DataFrame
consistent_days_df = pd.DataFrame(
    list(most_consistent_days.items()),
    columns=['Student ID', 'Most Consistent Payment Day']
)

# Merge with students_df
merged_df = pd.merge(students_df, consistent_days_df, on='Student ID', how='inner')
execution_time = time.time() - start_time

print(f"Execution Time (Serial): {execution_time:.2f} seconds")
print(merged_df.head())

# Save output
merged_df.to_csv("serial_output.csv", index=False)
