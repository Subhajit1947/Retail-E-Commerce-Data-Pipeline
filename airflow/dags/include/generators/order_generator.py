"""
Order Generator - Realistic Transaction Patterns

Key insight: Orders are generated from EXISTING customers and products.
This is how real e-commerce works.

Daily patterns:
- Weekend spike (1.5x)
- Month-end spike (salary day, 1.3x)
- Holiday season (1.8x)
- Sale events (2-3x)

Customer behavior:
- New customers: high order probability (exploring)
- Loyal customers: consistent ordering
- At-risk: low probability (need re-engagement)
"""
import random
import pandas as pd
from datetime import datetime, timedelta
from .base_generator import BaseGenerator
from ..config.data_config import (
    PAYMENT_METHODS, ORDER_PLATFORMS, SALE_EVENTS, CUSTOMER_SEGMENTS
)

class OrderGenerator(BaseGenerator):
    """
    Generates orders from existing customers and products.
    Reads customer/product data from DWH (or provided DataFrames).
    """
    
    def __init__(self, seed_date: datetime = None):
        super().__init__(seed_date)
    
    def _calculate_daily_order_count(self, target_date: datetime, 
                                      customer_count: int) -> int:
        """
        Calculate realistic daily order count.
        
        Formula: customer_count * base_rate * day_multipliers
        """
        base_rate = 0.05  # 5% of customers order daily
        count = int(customer_count * base_rate)
        
        # Weekend multiplier
        if target_date.weekday() >= 5:
            count = int(count * 1.5)
        
        # Month-end (salary day effect)
        if target_date.day >= 25:
            count = int(count * 1.3)
        
        # Holiday season
        if target_date.month in [10, 11, 12]:
            count = int(count * 1.8)
        
        # Sale events
        date_key = f"{target_date.month:02d}-{target_date.day:02d}"
        if date_key in SALE_EVENTS:
            count = int(count * SALE_EVENTS[date_key])
        
        # Add randomness
        count = int(count * random.uniform(0.9, 1.1))
        
        return max(1, count)
    
    def _select_customer_by_segment(self, customers_df: pd.DataFrame) -> pd.Series:
        """
        Select customer weighted by segment probability.
        New/Active customers order more frequently than dormant.
        """
        # Add order probability based on segment
        probabilities = []
        for _, customer in customers_df.iterrows():
            segment = customer.get('customer_segment', 'Active')
            prob = CUSTOMER_SEGMENTS.get(segment, {}).get('order_probability', 0.05)
            probabilities.append(prob)
        
        # Normalize
        total = sum(probabilities)
        probabilities = [p / total for p in probabilities]
        
        # Weighted random selection
        idx = random.choices(range(len(customers_df)), weights=probabilities, k=1)[0]
        return customers_df.iloc[idx]
    
    def _generate_order_datetime(self, target_date: datetime) -> datetime:
        """
        Generate realistic order timestamp.
        Peak hours: 12-2 PM (lunch), 8-10 PM (evening)
        """
        # Weighted hour selection
        hour_weights = [
            1, 1, 1, 1, 1, 2,      # 0-5 AM (early morning)
            4, 7, 9, 8, 7, 10,     # 6-11 AM (morning)
            12, 11, 9, 8, 9, 10,   # 12-5 PM (afternoon)
            13, 15, 14, 12, 8, 4   # 6-11 PM (evening)
        ]
        
        hour = random.choices(range(24), weights=hour_weights, k=1)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        return datetime.combine(target_date.date(), 
                                datetime.min.time()) + \
               timedelta(hours=hour, minutes=minute, seconds=second)
    
    def generate_daily_orders(self, target_date: datetime,
                               customers_df: pd.DataFrame,
                               products_df: pd.DataFrame) -> tuple:
        """
        Generate orders and order details for one day.
        
        Args:
            target_date: The date to generate orders for
            customers_df: DataFrame of active customers (from DWH)
            products_df: DataFrame of active products (from DWH)
        
        Returns:
            (orders_df, order_details_df)
        """
        if customers_df.empty or products_df.empty:
            print("⚠️ No customers or products available")
            return pd.DataFrame(), pd.DataFrame()
        
        # Calculate order count
        order_count = self._calculate_daily_order_count(
            target_date, len(customers_df)
        )
        
        print(f"🏗️ Generating {order_count} orders for {target_date.date()}...")
        
        orders = []
        order_details = []
        
        for i in range(order_count):
            # Select customer (weighted by segment)
            customer = self._select_customer_by_segment(customers_df)
            
            # Generate order
            order_uuid = self.generate_uuid()
            order_datetime = self._generate_order_datetime(target_date)
            
            # Payment method (weighted)
            payment = random.choices(
                list(PAYMENT_METHODS.keys()),
                weights=list(PAYMENT_METHODS.values()),
                k=1
            )[0]
            
            # Platform (weighted)
            platform = random.choices(
                list(ORDER_PLATFORMS.keys()),
                weights=list(ORDER_PLATFORMS.values()),
                k=1
            )[0]
            
            order = {
                'orderId': order_uuid,
                'customerId': customer['customerId'],
                'orderDate': order_datetime,
                'paymentMethod': payment,
                'orderPlatform': platform,
            }
            orders.append(order)
            
            # Generate order details (1-5 items)
            num_items = random.choices(
                [1, 2, 3, 4, 5],
                weights=[0.35, 0.30, 0.20, 0.10, 0.05]
            )[0]
            
            # Select products (weighted by price - cheaper products ordered more)
            product_weights = []
            for _, product in products_df.iterrows():
                # Inverse weighting: cheaper = more likely
                weight = 1.0 / max(product['price'], 100)
                product_weights.append(weight)
            
            selected_products = products_df.sample(
                n=min(num_items, len(products_df)),
                weights=product_weights
            )
            
            for _, product in selected_products.iterrows():
                quantity = random.choices(
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    weights=[0.40, 0.25, 0.15, 0.08, 0.05, 
                            0.03, 0.02, 0.01, 0.005, 0.005]
                )[0]

                detail = {
                    'orderDetailId': self.generate_uuid(),
                    'orderId': order_uuid,
                    'productId': product['productId'],
                    'Quantity': quantity
                }
                order_details.append(detail)
        
        # Calculate order totals
        orders_df = pd.DataFrame(orders)
        details_df = pd.DataFrame(order_details)
        
        orders_df = self.add_audit_columns(orders_df, 'I')
        details_df = self.add_audit_columns(details_df, 'I')
        
        print(f"✅ Generated {len(orders_df)} orders with {len(details_df)} items")
       
        
        return orders_df, details_df

