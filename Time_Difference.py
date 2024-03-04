import sqlite3
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# Read the SQL file
with open('pin_data_NYIT.sql', 'r') as file:
    sql_commands = file.read()

# Split the commands by semicolon
commands = sql_commands.split(';')

# Create an in-memory SQLite database
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

# Execute each command in the SQL file
for command in commands:
    cursor.execute(command)

# Queries to fetch data from tables
user_strings_query = "SELECT * FROM userStrings"
subjects_query = "SELECT * FROM subjects"
attempts_query = "SELECT * FROM attempts"

# Fetch data from the database into Pandas DataFrames
user_strings_df = pd.read_sql_query("SELECT * FROM userStrings", conn)
subjects_df = pd.read_sql_query("SELECT * FROM subjects", conn)
attempts_df = pd.read_sql_query("SELECT * FROM attempts", conn)

# Convert timestamp string to datetime in the 'attempts' DataFrame
attempts_df['time'] = pd.to_datetime(attempts_df['time'], format='%Y-%m-%dT%H:%M:%S.%fZ')

# Sort the DataFrame by 'userString' and then by 'time' in ascending order
attempts_df_sorted = attempts_df.sort_values(by=['userString', 'time'])

# After sorting the DataFrame by 'userString' and 'time'
attempts_df_sorted['time_difference'] = attempts_df_sorted.groupby('userString')['time'].diff().dt.total_seconds() * 1000

# Assuming 'key_pressed' indicates the key that was pressed and 'e' represents the 'e' key
# Flag the rows to be removed: those immediately following an 'e' keypress
to_remove = attempts_df_sorted['keyPressed'].shift(+1) == 'e'

# Separate the removed timings
removed_timings = attempts_df_sorted[to_remove]

# Remove the flagged timings from the original DataFrame
attempts_df_sorted_cleaned = attempts_df_sorted[~to_remove]

# Group the DataFrame by 'userString' and calculate the average time difference for each user
average_time_diff_per_user = attempts_df_sorted_cleaned.groupby('userString')['time_difference'].mean()

# Print the DataFrames
print("Removed Timings:")
print(removed_timings)

print("Modified DataFrame:")
print(attempts_df_sorted_cleaned)

# Print the results
print(average_time_diff_per_user)

# Saving the average time differences to a CSV file
average_csv_file_path = 'average_time_diff_per_user.csv'
average_time_diff_per_user.to_csv(average_csv_file_path, index=True)

# Save the DataFrame with 'e' keypress timings removed to a CSV file
csv_file_path_cleaned = 'attempts_cleaned.csv'
attempts_df_sorted_cleaned.to_csv(csv_file_path_cleaned, index=False)

# Save the DataFrame with removed timings to another CSV file
csv_file_path_removed = 'removed_timings.csv'
removed_timings.to_csv(csv_file_path_removed, index=False)


print(f"Cleaned DataFrame saved to {csv_file_path_cleaned}")
print(f"Removed timings DataFrame saved to {csv_file_path_removed}")
print(f"Average time differences per user saved to {average_csv_file_path}")

# Plotting a histogram of the average time differences per user
plt.figure(figsize=(20, 6))
plt.hist(average_time_diff_per_user, bins=20, color='blue', edgecolor='black')
plt.title('Histogram of Average Time Differences per User')
plt.xlabel('Average Time Difference (ms)')
plt.ylabel('Frequency')
plt.show()


# Close the connection
conn.close()

