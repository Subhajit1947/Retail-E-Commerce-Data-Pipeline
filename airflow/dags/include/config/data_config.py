"""
Realistic E-commerce Data Configuration
Based on actual e-commerce patterns
"""
from datetime import datetime, timedelta

# ==========================================
# INITIAL LOAD CONFIGURATION
# ==========================================
INITIAL_LOAD = {
    'customer_count': 10000,          # 10K customers over past year
    'product_count': 500,             # 500 products in catalog
    'order_history_days': 365,        # 1 year of historical orders
    'avg_daily_orders': 500,          # ~500 orders/day historically
}

# ==========================================
# DAILY GENERATION CONFIGURATION
# ==========================================
DAILY_CONFIG = {
    'new_customers': {
        'base': 30,                   # Minimum new signups/day
        'growth_rate': 0.02,         # 2% daily growth (viral effect)
        'weekend_multiplier': 1.3,   # More signups on weekends
        'campaign_multiplier': 2.0,    # During marketing campaigns
        'max_daily': 200,              # Cap at 200/day
    },
    'product_updates': {
        'new_products_weekly': 3,    # New products added per week
        'price_change_rate': 0.05,   # 5% of products change price weekly
        'discount_probability': 0.15, # 15% chance of sale event
    },
    'orders': {
        'base_per_customer': 0.05,   # 5% of customers order daily
        'weekend_multiplier': 1.5,   # Weekend shopping spike
        'month_end_multiplier': 1.3, # Salary day effect
        'holiday_months': [10, 11, 12],
        'holiday_multiplier': 1.8,
        'avg_items_per_order': (1, 5),
        'quantity_range': (1, 10),
    }
}

# ==========================================
# REALISTIC PATTERNS
# ==========================================
PAYMENT_METHODS = {
    'Credit Card': 0.35,
    'Debit Card': 0.25,
    'UPI': 0.22,
    'Net Banking': 0.10,
    'Cash on Delivery': 0.06,
    'Wallet': 0.02,
}

ORDER_PLATFORMS = {
    'Mobile App': 0.55,
    'Website': 0.28,
    'Mobile Web': 0.12,
    'Partner API': 0.05,
}

PRODUCT_CATEGORIES = [
    'Electronics', 'Clothing', 'Home & Kitchen', 'Books',
    'Sports & Fitness', 'Toys & Games', 'Beauty & Personal Care',
    'Automotive', 'Food & Grocery', 'Office Supplies'
]

# Seasonal sale events (MM-DD format)
SALE_EVENTS = {
    '01-01': 2.0,   # New Year
    '02-14': 1.8,   # Valentine's
    '03-08': 1.5,   # Women's Day
    '05-01': 1.4,   # Labor Day
    '07-15': 1.9,   # Mid-year sale
    '08-15': 1.6,   # Independence Day
    '10-02': 1.5,   # Gandhi Jayanti
    '11-11': 2.5,   # Singles Day / Diwali
    '11-29': 2.2,   # Black Friday
    '12-25': 1.7,   # Christmas
}

# Customer lifecycle stages
CUSTOMER_SEGMENTS = {
    'New': {'days_since_signup': (0, 30), 'order_probability': 0.15},
    'Active': {'days_since_signup': (31, 180), 'order_probability': 0.08},
    'Loyal': {'days_since_signup': (181, 365), 'order_probability': 0.12},
    'At Risk': {'days_since_signup': (366, 730), 'order_probability': 0.03},
    'Dormant': {'days_since_signup': (731, 9999), 'order_probability': 0.01},
}
