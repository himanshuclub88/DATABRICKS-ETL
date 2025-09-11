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
FROM workspace.development.sales_data;