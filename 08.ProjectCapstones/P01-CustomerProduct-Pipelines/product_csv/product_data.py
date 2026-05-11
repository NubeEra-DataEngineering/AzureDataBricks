import pandas as pd
import random

# Generate sample data
num_products = 50
categories = ['Electronics', 'Clothing', 'Home Appliances', 'Books', 'Sports']

data = []
for i in range(1, num_products + 1):
    product_id = i
    product_name = f'Product {i}'
    category = random.choice(categories)
    price = round(random.uniform(10, 500), 2)
    data.append((product_id, product_name, category, price))

# Create a DataFrame
columns = ['product_id', 'product_name', 'category', 'price']
df = pd.DataFrame(data, columns=columns)

# Save the DataFrame to a CSV file
csv_file_path = 'product_data.csv'
df.to_csv(csv_file_path, index=False)

print(f"Sample dataset saved to {csv_file_path}")
