import pandas as pd
import random

# Generate sample customer data
num_customers = 100

first_names = [
    'John', 'Jane', 'Michael', 'Susan', 'Robert',
    'Emily', 'David', 'Sarah', 'Daniel', 'Laura'
]

last_names = [
    'Doe', 'Smith', 'Johnson', 'Lee', 'Williams',
    'Brown', 'Davis', 'Miller', 'Wilson', 'Taylor'
]

data = []

for i in range(1, num_customers + 1):
    customer_id = f'CUST{i:03d}'

    first_name = random.choice(first_names)
    last_name = random.choice(last_names)

    email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"

    phone = (
        f"{random.randint(100, 999)}-"
        f"{random.randint(100, 999)}-"
        f"{random.randint(1000, 9999)}"
    )

    data.append((
        customer_id,
        first_name,
        last_name,
        email,
        phone
    ))

# Create DataFrame
columns = [
    'customer_id',
    'first_name',
    'last_name',
    'email',
    'phone'
]

df = pd.DataFrame(data, columns=columns)

# Save to CSV
csv_file_path = 'customer_data.csv'
df.to_csv(csv_file_path, index=False)

print(f"Sample dataset saved to {csv_file_path}")