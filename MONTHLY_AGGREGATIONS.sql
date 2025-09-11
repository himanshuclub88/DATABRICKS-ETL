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
FROM workspace.development.sales_data
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;