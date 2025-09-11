-- =====================================================
-- 1. BASIC AGGREGATIONS
-- =====================================================

-- Total sales summary
SELECT 
    COUNT(*) as total_orders,
    SUM(quantity) as total_quantity_sold,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MIN(total_amount) as min_order_value,
    MAX(total_amount) as max_order_value,
    AVG(discount_percent) as avg_discount_percent
FROM liu_798539033393980.testing.sales_data;

-- =====================================================
-- 2. TIME-BASED AGGREGATIONS
-- =====================================================

-- Monthly sales trends
SELECT 
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as orders_count,
    SUM(total_amount) as monthly_revenue,
    AVG(total_amount) as avg_order_value,
    SUM(quantity) as total_units_sold
FROM liu_798539033393980.testing.sales_data
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;

-- Daily sales for the last 30 days
SELECT 
    order_date,
    COUNT(*) as daily_orders,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_daily_order_value
FROM liu_798539033393980.testing.sales_data
WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY order_date
ORDER BY order_date DESC;

-- Quarterly performance
SELECT 
    YEAR(order_date) as year,
    QUARTER(order_date) as quarter,
    COUNT(*) as orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM liu_798539033393980.testing.sales_data
GROUP BY YEAR(order_date), QUARTER(order_date)
ORDER BY year, quarter;

-- =====================================================
-- 3. PRODUCT-BASED AGGREGATIONS
-- =====================================================

-- Product performance analysis
SELECT 
    product_name,
    COUNT(*) as orders_count,
    SUM(quantity) as total_units_sold,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    AVG(unit_price) as avg_unit_price,
    AVG(discount_percent) as avg_discount
FROM liu_798539033393980.testing.sales_data
GROUP BY product_name
ORDER BY total_revenue DESC;

-- Top 5 products by revenue
SELECT 
    product_name,
    SUM(total_amount) as total_revenue,
    COUNT(*) as orders_count,
    SUM(quantity) as total_units_sold
FROM liu_798539033393980.testing.sales_data
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 5;

-- =====================================================
-- 4. REGIONAL AGGREGATIONS
-- =====================================================

-- Regional performance analysis
SELECT 
    region,
    COUNT(*) as orders_count,
    SUM(total_amount) as regional_revenue,
    AVG(total_amount) as avg_order_value,
    SUM(quantity) as total_units_sold,
    COUNT(DISTINCT sales_rep) as active_sales_reps
FROM liu_798539033393980.testing.sales_data
GROUP BY region
ORDER BY regional_revenue DESC;

-- Regional product preferences
SELECT 
    region,
    product_name,
    COUNT(*) as orders_count,
    SUM(total_amount) as product_revenue,
    RANK() OVER (PARTITION BY region ORDER BY SUM(total_amount) DESC) as revenue_rank
FROM liu_798539033393980.testing.sales_data
GROUP BY region, product_name
QUALIFY revenue_rank <= 3
ORDER BY region, revenue_rank;

-- =====================================================
-- 5. SALES REPRESENTATIVE ANALYSIS
-- =====================================================

-- Sales rep performance
SELECT 
    sales_rep,
    COUNT(*) as orders_handled,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_order_value,
    SUM(quantity) as total_units_sold,
    AVG(discount_percent) as avg_discount_given
FROM liu_798539033393980.testing.sales_data
GROUP BY sales_rep
ORDER BY total_sales DESC;

-- Top performing sales reps by region
SELECT 
    region,
    sales_rep,
    SUM(total_amount) as total_sales,
    COUNT(*) as orders_count,
    RANK() OVER (PARTITION BY region ORDER BY SUM(total_amount) DESC) as sales_rank
FROM liu_798539033393980.testing.sales_data
GROUP BY region, sales_rep
QUALIFY sales_rank <= 2
ORDER BY region, sales_rank;

-- =====================================================
-- 6. CUSTOMER TYPE ANALYSIS
-- =====================================================

-- Customer type performance
SELECT 
    customer_type,
    COUNT(*) as orders_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    AVG(quantity) as avg_quantity_per_order,
    AVG(discount_percent) as avg_discount_received
FROM liu_798539033393980.testing.sales_data
GROUP BY customer_type
ORDER BY total_revenue DESC;

-- =====================================================
-- 7. ADVANCED AGGREGATIONS & ANALYTICS
-- =====================================================

-- Revenue concentration analysis (80/20 rule)
WITH product_revenue AS (
    SELECT 
        product_name,
        SUM(total_amount) as product_revenue
    FROM liu_798539033393980.testing.sales_data
    GROUP BY product_name
),
revenue_ranked AS (
    SELECT 
        product_name,
        product_revenue,
        SUM(product_revenue) OVER () as total_revenue,
        PERCENT_RANK() OVER (ORDER BY product_revenue DESC) as revenue_percentile
    FROM product_revenue
)
SELECT 
    product_name,
    product_revenue,
    (product_revenue / total_revenue * 100) as revenue_percentage,
    revenue_percentile
FROM revenue_ranked
WHERE revenue_percentile <= 0.2  -- Top 20% of products
ORDER BY product_revenue DESC;

-- Monthly growth rate analysis
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        SUM(total_amount) as monthly_revenue
    FROM liu_798539033393980.testing.sales_data
    GROUP BY DATE_TRUNC('month', order_date)
),
growth_analysis AS (
    SELECT 
        month,
        monthly_revenue,
        LAG(monthly_revenue) OVER (ORDER BY month) as prev_month_revenue,
        ((monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month)) 
         / LAG(monthly_revenue) OVER (ORDER BY month) * 100) as growth_rate
    FROM monthly_sales
)
SELECT 
    month,
    monthly_revenue,
    prev_month_revenue,
    ROUND(growth_rate, 2) as growth_rate_percent
FROM growth_analysis
WHERE prev_month_revenue IS NOT NULL
ORDER BY month;

-- Discount impact analysis
SELECT 
    CASE 
        WHEN discount_percent = 0 THEN 'No Discount'
        WHEN discount_percent <= 10 THEN 'Low Discount (1-10%)'
        WHEN discount_percent <= 20 THEN 'Medium Discount (11-20%)'
        ELSE 'High Discount (>20%)'
    END as discount_category,
    COUNT(*) as orders_count,
    AVG(total_amount) as avg_order_value,
    SUM(total_amount) as total_revenue,
    AVG(quantity) as avg_quantity
FROM liu_798539033393980.testing.sales_data
GROUP BY 
    CASE 
        WHEN discount_percent = 0 THEN 'No Discount'
        WHEN discount_percent <= 10 THEN 'Low Discount (1-10%)'
        WHEN discount_percent <= 20 THEN 'Medium Discount (11-20%)'
        ELSE 'High Discount (>20%)'
    END
ORDER BY avg_order_value DESC;

-- =====================================================
-- 8. WINDOW FUNCTIONS & RUNNING TOTALS
-- =====================================================

-- Running total of daily sales
SELECT 
    order_date,
    SUM(total_amount) as daily_sales,
    SUM(SUM(total_amount)) OVER (ORDER BY order_date) as running_total
FROM liu_798539033393980.testing.sales_data
GROUP BY order_date
ORDER BY order_date;

-- Moving average of sales (7-day window)
WITH daily_sales AS (
    SELECT 
        order_date,
        SUM(total_amount) as daily_revenue
    FROM liu_798539033393980.testing.sales_data
    GROUP BY order_date
)
SELECT 
    order_date,
    daily_revenue,
    AVG(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7_days
FROM daily_sales
ORDER BY order_date;
