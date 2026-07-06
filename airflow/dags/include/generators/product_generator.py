"""
Product Generator - Realistic E-commerce Catalog

Day 0: Generate 500 products across categories
Weekly: Add 3 new products
Daily: 5% of products get price changes (SCD Type 2)

Realistic patterns:
- Electronics: higher prices, frequent updates
- Clothing: seasonal, mid-range prices
- Food: low prices, stable
"""
import random
import pandas as pd
from datetime import datetime, timedelta
from .base_generator import BaseGenerator
from ..config.data_config import PRODUCT_CATEGORIES
from faker import Faker
fake = Faker(['en_IN'])
class ProductGenerator(BaseGenerator):
    """
    Generates product catalog with realistic pricing and update patterns.
    """
    
    # Price ranges by category (min, max, typical)
    PRICE_RANGES = {
        'Electronics': (500, 100000, 15000),
        'Clothing': (200, 15000, 2000),
        'Home & Kitchen': (300, 50000, 5000),
        'Books': (50, 5000, 500),
        'Sports & Fitness': (300, 30000, 3000),
        'Toys & Games': (100, 10000, 1500),
        'Beauty & Personal Care': (50, 10000, 800),
        'Automotive': (200, 50000, 5000),
        'Food & Grocery': (10, 5000, 300),
        'Office Supplies': (50, 20000, 1000),
    }
    
    def __init__(self, seed_date: datetime = None):
        super().__init__(seed_date)
    
    def _generate_single_product(self, category: str = None) -> dict:
        """Generate one product"""
        
        product_uuid = self.generate_uuid()
        
        if category is None:
            category = random.choice(PRODUCT_CATEGORIES)
        
        
        price_range = self.PRICE_RANGES[category]
        
        # Generate price using log-normal distribution (realistic)
        import numpy as np
        price = np.random.lognormal(
            mean=np.log(price_range[2]), 
            sigma=0.5
        )
        price = max(price_range[0], min(price_range[1], price))
        
        # Round to psychological pricing (₹999, ₹1499, etc.)
        if price > 1000:
            price = round(price / 100) * 100 - 1
        else:
            price = round(price / 10) * 10 - 1
        
        # Generate brand name
        brand = fake.company().split()[0]
        
        product = {
            'productId': product_uuid,
            'productName': f"{brand} {category} {fake.word().capitalize()}",
            'brandName': brand,
            'productDescription': fake.text(max_nb_chars=200),
            'price': round(price, 2),
            'productCategory': category
        }
        return product
    
    def generate_initial_catalog(self, count: int = 500) -> pd.DataFrame:
        """Generate initial product catalog"""
        print(f"🏗️ Generating {count} products...")
        
        products = []
        # Distribute across categories
        products_per_category = count // len(PRODUCT_CATEGORIES)
        
        for category in PRODUCT_CATEGORIES:
            for _ in range(products_per_category):
                product = self._generate_single_product(category)
                products.append(product)
        
        df = pd.DataFrame(products)
        df = self.add_audit_columns(df, 'I')
        
        print(f"✅ Generated {len(df)} products across {len(PRODUCT_CATEGORIES)} categories")
        print(f"   Price range: ₹{df['price'].min():,.0f} - ₹{df['price'].max():,.0f}")
        print(f"   Avg price: ₹{df['price'].mean():,.0f}")
        return df
    
    def generate_new_products(self, target_date: datetime, count: int = 3) -> pd.DataFrame:
        """Generate new products for a week"""
        print(f"🏗️ Generating {count} new products for week of {target_date.date()}...")
        
        products = [self._generate_single_product() 
                    for _ in range(count)]
        
        df = pd.DataFrame(products)
        df = self.add_audit_columns(df, 'I')
        
        print(f"✅ Generated {len(df)} new products")
        return df
    
    def generate_price_updates(self, existing_products: pd.DataFrame, 
                                target_date: datetime,
                                update_rate: float = 0.05) -> pd.DataFrame:
        """
        Generate price changes for SCD Type 2.
        
        Realistic: 5% of products change price weekly
        - Electronics: frequent small changes
        - Clothing: seasonal sales
        - Food: stable
        """
        if existing_products.empty:
            return pd.DataFrame()
        
        update_count = max(1, int(len(existing_products) * update_rate))
        products_to_update = existing_products.sample(n=update_count)
        
        print(f"💰 Generating {update_count} price updates for {target_date.date()}...")
        
        updates = []
        for _, product in products_to_update.iterrows():
            updated = product.copy()
            
            # Different change patterns by category
            category = product['product_category']
            
            if category in ['Electronics']:
                # Small frequent changes (2-10%)
                change = random.uniform(-0.10, 0.10)
            elif category in ['Clothing', 'Beauty & Personal Care']:
                # Seasonal sales (10-30% off)
                change = random.uniform(-0.30, 0.05)
            else:
                # Stable (0-5%)
                change = random.uniform(-0.05, 0.05)
            
            new_price = updated['price'] * (1 + change)
            updated['price'] = round(max(1, new_price), 2)
            updated['op'] = 'U'
            updated['updated_at'] = datetime.now()
            
            updates.append(updated)
        
        df = pd.DataFrame(updates)
        df = self.add_audit_columns(df, 'U')
        
        print(f"✅ Generated {len(df)} price updates")
        return df

