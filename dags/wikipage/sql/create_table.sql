-- SQL script to create the wikipedia_pageviews table
CREATE TABLE IF NOT EXISTS wikipedia_pageviews (
    company VARCHAR(55),
    page_name VARCHAR(255),
    view_count INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);