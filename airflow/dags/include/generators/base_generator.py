
import uuid
import random
import string
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import numpy as np

# Use Indian locale for realistic data
fake = Faker(['en_IN'])

class BaseGenerator:
    """
    Stateless base generator using UUID for all IDs.
    No state files. No sequential counters. Fully distributed.
    """
    
    def __init__(self, seed_date: datetime = None):
        """
        Args:
            seed_date: Date used for deterministic generation.
                       Same date = same data (idempotent).
        """
        self.seed_date = seed_date or datetime.now()
        # Use date as seed for reproducibility
        random.seed(self.seed_date.strftime('%Y%m%d'))
        np.random.seed(int(self.seed_date.strftime('%Y%m%d')))
        fake.seed_instance(int(self.seed_date.strftime('%Y%m%d')))
    
    @staticmethod
    def generate_uuid() -> str:
        """Generate a random UUID v4"""
        return str(uuid.uuid4())
    
    @staticmethod
    def generate_deterministic_uuid(seed_string: str) -> str:
        """
        Generate deterministic UUID v5 from a seed string.
        Same input always produces same UUID.
        """
        return str(uuid.uuid5(uuid.NAMESPACE_OID, seed_string))
    
    def generate_phone(self) -> str:
        """Generate valid Indian mobile number"""
        prefix = random.choice(['6', '7', '8', '9'])
        number = prefix + ''.join(random.choices(string.digits, k=9))
        return number
    
    def generate_email(self, name: str) -> str:
        """Generate realistic email from name"""
        clean = name.lower().replace(' ', '.').replace('-', '')
        domains = [
            'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com',
            'icloud.com', 'rediffmail.com', 'protonmail.com'
        ]
        
        patterns = [
            f"{clean}@{random.choice(domains)}",
            f"{clean}{random.randint(1, 999)}@{random.choice(domains)}",
            f"{clean}.{random.randint(10, 99)}@{random.choice(domains)}",
            f"{clean}_{random.randint(1, 99)}@{random.choice(domains)}",
        ]
        return random.choice(patterns)
    
    def generate_address(self) -> dict:
        """Generate complete address as dict"""
        return {
            'street': fake.street_address(),
            'city': fake.city(),
            'state': fake.state(),
            'zipcode': fake.postcode(),
            'country': 'India'
        }
    
    def add_audit_columns(self, df: pd.DataFrame, operation: str = 'I') -> pd.DataFrame:
        """Add standard audit columns"""
        df['op'] = operation
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        df['batch_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
        df['source'] = 'data_generator'
        return df

