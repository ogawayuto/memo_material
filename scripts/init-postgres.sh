#!/bin/bash
# Initialize PostgreSQL database manually

echo "Initializing PostgreSQL database..."

# Wait for PostgreSQL to be ready
until docker exec postgres pg_isready -U postgres > /dev/null 2>&1; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

echo "PostgreSQL is ready. Running initialization script..."

# Execute init script
docker exec -i postgres psql -U postgres << 'EOF'
-- Create source database (if not exists)
SELECT 'CREATE DATABASE sourcedb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sourcedb')\gexec

-- Connect to the source database
\c sourcedb;

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set REPLICA IDENTITY FULL for CDC
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Insert sample data
INSERT INTO customers (name, email) VALUES
    ('John Doe', 'john.doe@example.com'),
    ('Jane Smith', 'jane.smith@example.com'),
    ('Alice Johnson', 'alice.johnson@example.com')
ON CONFLICT (email) DO NOTHING;

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for updated_at
DROP TRIGGER IF EXISTS update_customers_updated_at ON customers;
CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Display the created table and data
SELECT 'Database initialized successfully!' AS status;
SELECT * FROM customers;
EOF

echo "PostgreSQL initialization complete!"
