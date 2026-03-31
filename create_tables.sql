CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    loyalty_tier TEXT,
    order_count INT,
    lifetime_value NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS categories (
    id INT PRIMARY KEY,
    name TEXT,
    description TEXT,
    parent_category TEXT,
    product_count INT
);

CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    brand TEXT,
    price NUMERIC(10,2),
    description TEXT,
    stock_quantity INT,
    avg_rating NUMERIC(3,2),
    review_count INT
);

CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY,
    order_id TEXT,
    customer_id INT,
    customer_name TEXT,
    customer_email TEXT,
    total_amount NUMERIC(10,2),
    status TEXT,
    item_count INT,
    shipping_city TEXT,
    shipping_state TEXT,
    payment_method TEXT
);

CREATE TABLE IF NOT EXISTS reviews (
    id INT PRIMARY KEY,
    product_id INT,
    product_name TEXT,
    customer_id INT,
    customer_name TEXT,
    rating INT,
    review_text TEXT,
    helpful_count INT
);

CREATE TABLE IF NOT EXISTS users (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name TEXT,
    email TEXT
);
