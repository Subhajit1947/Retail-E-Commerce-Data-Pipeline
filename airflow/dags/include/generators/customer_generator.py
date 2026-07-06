"""
Customer Generator - Realistic E-commerce Patterns

Day 0: Generate 10K historical customers spread over past year
Daily: Generate 30-200 new customers with realistic signup patterns

SCD Type 2: When customer updates profile, new version created with same customer_id (UUID)
"""
import random
import pandas as pd
from datetime import datetime, timedelta
from .base_generator import BaseGenerator
from ..config.data_config import CUSTOMER_SEGMENTS
from faker import Faker
fake = Faker(['en_IN'])

class CustomerGenerator(BaseGenerator):
    """
    Generates customers with realistic e-commerce patterns.
    
    Real-world behavior:
    - Customers sign up once (mostly)
    - Occasionally update address, phone, email
    - Some churn (become inactive)
    """
    
    def __init__(self, seed_date: datetime = None):
        super().__init__(seed_date)
    
    def _generate_single_customer(self, signup_date: datetime, is_historical: bool = False) -> dict:
        """Generate one realistic customer profile"""
        
        # Generate UUID for this customer (permanent)
        customer_uuid = self.generate_uuid()
        
        name = fake.name()
        address = self.generate_address()
        
        customer = {
            'customerId': customer_uuid,          # UUID - never changes
            'name': name,
            'email': self.generate_email(name),
            'phone': self.generate_phone(),
            'address': address['street'],
            'city': address['city'],
            'state': address['state'],
            'zipcode': address['zipcode'],
            'country': address['country'],
            'signup_date': signup_date.date(),
            
        }
        return customer
    
    def generate_initial_customers(self, count: int = 10000, days_back: int = 365) -> pd.DataFrame:
        """
        Generate initial customer base spread over past year.
        
        Realistic pattern: More recent signups than old ones.
        (E-commerce growth curve)
        """
        print(f"🏗️ Generating {count} initial customers over {days_back} days...")
        
        customers = []
        base_date = datetime.now() - timedelta(days=days_back)
        
        for i in range(count):
            # Exponential distribution: more customers in recent months
            # 70% of customers signed up in last 6 months
            days_offset = int(random.expovariate(1.0 / (days_back * 0.3)))
            days_offset = min(days_offset, days_back - 1)
            
            signup_date = base_date + timedelta(days=days_offset)
            customer = self._generate_single_customer(signup_date, is_historical=True)
            customers.append(customer)
        
        df = pd.DataFrame(customers)
        df = self.add_audit_columns(df, 'I')
        
        print(f"✅ Generated {len(df)} customers")
        print(f"   Signup range: {df['signup_date'].min()} to {df['signup_date'].max()}")
        return df
    
    def generate_daily_new_customers(self, target_date: datetime, day_number: int = 1) -> pd.DataFrame:
        """
        Generate new customers for a specific day.
        
        Realistic patterns:
        - Weekend signup spike (browsing time)
        - Campaign days (2x)
        - Growth curve (more signups as business grows)
        """
        from ..config.data_config import DAILY_CONFIG
        
        config = DAILY_CONFIG['new_customers']
        
        # Base count with growth
        base_count = config['base']
        growth = int(base_count * config['growth_rate'] * day_number)
        count = min(base_count + growth, config['max_daily'])
        
        # Weekend multiplier
        if target_date.weekday() >= 5:  # Saturday/Sunday
            count = int(count * config['weekend_multiplier'])
        
        # Campaign days (random 10% chance)
        if random.random() < 0.1:
            count = int(count * config['campaign_multiplier'])
        
        # Add noise
        count = max(0, int(count * random.uniform(0.8, 1.2)))
        
        if count == 0:
            return pd.DataFrame()
        
        print(f"🏗️ Generating {count} new customers for {target_date.date()}...")
        
        customers = [self._generate_single_customer(target_date) for _ in range(count)]
        
        df = pd.DataFrame(customers)
        df = self.add_audit_columns(df, 'I')
        
        print(f"✅ Generated {len(df)} new customers")
        return df
    
    def generate_customer_updates(self, existing_customers: pd.DataFrame, 
                                   target_date: datetime, 
                                   update_rate: float = 0.02) -> pd.DataFrame:
        """
        Generate customer profile updates for SCD Type 2.
        
        Realistic: 2% of customers update profile daily
        (change address, phone, email)
        """
        if existing_customers.empty:
            return pd.DataFrame()
        
        update_count = max(1, int(len(existing_customers) * update_rate))
        customers_to_update = existing_customers.sample(n=update_count)
        
        print(f"🔄 Generating {update_count} customer updates for {target_date.date()}...")
        
        updates = []
        for _, customer in customers_to_update.iterrows():
            updated = customer.copy()
            
            # Realistic update types
            update_type = random.choice(['address', 'phone', 'email', 'multiple'])
            
            if update_type in ['address', 'multiple']:
                address = self.generate_address()
                updated['address'] = address['street']
                updated['city'] = address['city']
                updated['state'] = address['state']
                updated['zipcode'] = address['zipcode']
            
            if update_type in ['phone', 'multiple']:
                updated['phone'] = self.generate_phone()
            
            if update_type in ['email', 'multiple']:
                updated['email'] = self.generate_email(updated['name'])
            
            updated['op'] = 'U'
            updated['updated_at'] = datetime.now()
            updates.append(updated)
        
        df = pd.DataFrame(updates)
        df = self.add_audit_columns(df, 'U')
        
        print(f"✅ Generated {len(df)} customer updates")
        return df

