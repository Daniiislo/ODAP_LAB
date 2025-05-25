import csv
import random
import os

# Define possible product prefixes and suffixes to generate names
prefixes = ["Super", "Ultra", "Mega", "Pro", "Classic", "Premium", "Elite", "Eco", "Smart", "Royal"]
categories = ["Electronics", "Clothing", "Home", "Food", "Toys", "Sports"]
products = ["TV", "Laptop", "Phone", "Computer", "Pants", "Shirt", "Shoes", "Bag", "Pot", "Pan", 
            "Table", "Chair", "Lamp", "Milk", "Cake", "Candy", "Water", "Comic", "Book", "Cream", "Lipstick", 
            "Shampoo", "Doll", "Car", "Ball", "Racket", "Weight", "Treadmill"]

# Function to generate a random product name
def generate_product_name():
    if random.random() < 0.7:  # 70% chance to have a prefix
        return f"{random.choice(prefixes)} {random.choice(products)} {random.randint(1, 100)}"
    else:
        return f"{random.choice(products)} {random.randint(1, 100)}"

# Output file path
output_file = "products.csv"

# Generate 1000 records
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Name', 'Price', 'Quantity', 'Categories']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for _ in range(1000):
        # Generate random product data
        # Price distribution: 60% under 100, 40% over 100
        price = random.randint(10, 90) if random.random() < 0.6 else random.randint(100, 500)
        
        writer.writerow({
            'Name': generate_product_name(),
            'Price': price,
            'Quantity': random.randint(1, 100),
            'Categories': random.choice(categories)
        })

print(f"Sample CSV file with 1000 products created at: {output_file}")
