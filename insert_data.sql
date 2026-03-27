-- Insert 1000 records into customers table
INSERT INTO customers (id, first_name, last_name, email, phone, city, state, country, loyalty_tier, order_count, lifetime_value)
SELECT 
    i,
    'FirstName' || i,
    'LastName' || i,
    'customer' || i || '@example.com',
    '+1-555-' || LPAD(i::text, 7, '0'),
    (ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Denver', 'Seattle', 'Boston', 'Miami', 'Atlanta', 'Portland'])[1 + (i % 15)],
    (ARRAY['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CO', 'WA', 'MA', 'FL', 'GA', 'OR'])[1 + (i % 15)],
    'USA',
    (ARRAY['Bronze', 'Silver', 'Gold', 'Platinum'])[1 + (i % 4)],
    (i % 50),
    (i * 10.50)::numeric(10,2)
FROM generate_series(1, 1000) AS i
ON CONFLICT (id) DO NOTHING;

-- Insert 1000 records into categories table
INSERT INTO categories (id, name, description, parent_category, product_count)
SELECT 
    i,
    'Category' || i,
    'Description for category ' || i,
    CASE WHEN i % 3 = 0 THEN 'Parent' || ((i / 3) % 10) ELSE NULL END,
    (i % 100)
FROM generate_series(1, 1000) AS i
ON CONFLICT (id) DO NOTHING;

-- Insert 1000 records into products table
INSERT INTO products (id, name, category, brand, price, description, stock_quantity, avg_rating, review_count)
SELECT 
    i,
    'Product' || i,
    (ARRAY['Electronics', 'Clothing', 'Home & Kitchen', 'Sports', 'Books', 'Toys', 'Beauty', 'Automotive', 'Garden', 'Health'])[1 + (i % 10)],
    'Brand' || ((i % 50) + 1),
    (19.99 + (i % 500))::numeric(10,2),
    'High quality product ' || i || ' with excellent features',
    (i % 1000),
    (3.0 + (i % 3))::numeric(3,2),
    (i % 200)
FROM generate_series(1, 1000) AS i
ON CONFLICT (id) DO NOTHING;

-- Insert 1000 records into orders table
INSERT INTO orders (id, order_id, customer_id, customer_name, customer_email, total_amount, status, item_count, shipping_city, shipping_state, payment_method)
SELECT 
    i,
    'ORD-' || LPAD(i::text, 8, '0'),
    1 + (i % 1000),
    'Customer' || (1 + (i % 1000)),
    'customer' || (1 + (i % 1000)) || '@example.com',
    (50.00 + (i % 500))::numeric(10,2),
    (ARRAY['pending', 'processing', 'shipped', 'delivered', 'cancelled'])[1 + (i % 5)],
    1 + (i % 10),
    (ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Denver'])[1 + (i % 10)],
    (ARRAY['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CO'])[1 + (i % 10)],
    (ARRAY['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay'])[1 + (i % 5)]
FROM generate_series(1, 1000) AS i
ON CONFLICT (id) DO NOTHING;

-- Insert 1000 records into reviews table
INSERT INTO reviews (id, product_id, product_name, customer_id, customer_name, rating, review_text, helpful_count)
SELECT 
    i,
    1 + (i % 1000),
    'Product' || (1 + (i % 1000)),
    1 + (i % 1000),
    'Customer' || (1 + (i % 1000)),
    1 + (i % 5),
    (ARRAY[
        'Excellent product! Highly recommend.',
        'Good quality for the price.',
        'Average product, nothing special.',
        'Not satisfied with the quality.',
        'Outstanding! Exceeded my expectations.',
        'Great value for money.',
        'Decent product but could be better.',
        'Very disappointed with this purchase.',
        'Amazing quality and fast shipping!',
        'Perfect for my needs.'
    ])[1 + (i % 10)],
    (i % 100)
FROM generate_series(1, 1000) AS i
ON CONFLICT (id) DO NOTHING;

-- Insert 1000 records into users table
INSERT INTO users (name, email)
SELECT 
    'User' || i,
    'user' || i || '@example.com'
FROM generate_series(1, 1000) AS i;
