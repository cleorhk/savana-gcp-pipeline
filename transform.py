import json
import pandas as pd

# Load the JSON files
def load_data():
    with open('data/users.json') as f:
        users_data = json.load(f)
    with open('data/products.json') as f:
        products_data = json.load(f)
    with open('data/carts.json') as f:
        carts_data = json.load(f)
    return users_data, products_data, carts_data

# Transform Users Table
def transform_users(users_data):
    users = []
    for user in users_data['users']:
        address = user.get('address', {})
        users.append({
            'user_id': user['id'],
            'first_name': user['firstName'],
            'last_name': user['lastName'],
            'gender': user['gender'],
            'age': user['age'],
            'street': address.get('street', ''),
            'city': address.get('city', ''),
            'postal_code': address.get('postalCode', '')
        })
    return pd.DataFrame(users)

# Transform Products Table
def transform_products(products_data):
    products = []
    for product in products_data['products']:
        if product['price'] > 50:  # Exclude products with price <= 50
            products.append({
                'product_id': product['id'],
                'name': product['title'],
                'category': product['category'],
                'brand': product['brand'],
                'price': product['price']
            })
    return pd.DataFrame(products)

# Transform Carts Table
def transform_carts(carts_data):
    carts = []
    for cart in carts_data['carts']:
        total_cart_value = sum(product['discountedTotal'] for product in cart['products'])
        for product in cart['products']:
            carts.append({
                'cart_id': cart['id'],
                'user_id': cart['userId'],
                'product_id': product['id'],
                'quantity': product['quantity'],
                'price': product['price'],
                'total_cart_value': total_cart_value
            })
    return pd.DataFrame(carts)

# Main function to execute transformations
def main():
    users_data, products_data, carts_data = load_data()

    # Transform data
    users_df = transform_users(users_data)
    products_df = transform_products(products_data)
    carts_df = transform_carts(carts_data)

    # Save transformed data to CSV files
    users_df.to_csv('data/cleaned_users.csv', index=False)
    products_df.to_csv('data/cleaned_products.csv', index=False)
    carts_df.to_csv('data/cleaned_carts.csv', index=False)

    print("Data transformation and export completed successfully.")

if __name__ == "__main__":
    main()
