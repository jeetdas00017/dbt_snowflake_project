CREATE TABLE IF NOT EXISTS public.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    acquisition_channel VARCHAR(50),
    signup_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES public.customers(customer_id),
    product_id INTEGER,
    order_date DATE,
    total_amount DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS public.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    price DECIMAL(10, 2),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL
);

INSERT INTO public.customers (first_name, last_name, email, phone, city, state, country, acquisition_channel, signup_date) VALUES
('John', 'Doe', 'john.doe@example.com', '555-0001', 'New York', 'NY', 'USA', 'organic', '2025-01-01'),
('Jane', 'Smith', 'jane.smith@example.com', '555-0002', 'Los Angeles', 'CA', 'USA', 'paid_ads', '2025-02-01'),
('Bob', 'Johnson', 'bob.johnson@example.com', '555-0003', 'Chicago', 'IL', 'USA', 'referral', '2025-03-01');

INSERT INTO public.products (name, description, price, category) VALUES
('Laptop', 'High-performance laptop', 999.99, 'Electronics'),
('Mouse', 'Wireless mouse', 29.99, 'Electronics'),
('Keyboard', 'Mechanical keyboard', 79.99, 'Electronics');

INSERT INTO public.orders (customer_id, product_id, order_date, total_amount, status) VALUES
(1, 1, '2025-06-01', 999.99, 'completed'),
(1, 2, '2025-06-05', 29.99, 'completed'),
(2, 2, '2025-06-10', 29.99, 'pending'),
(3, 3, '2025-06-15', 79.99, 'completed');
