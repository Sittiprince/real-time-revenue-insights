import json
from typing import Dict, Tuple, Union
import logging
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger(__name__)

class CurrencyConverter:
    def __init__(self):
        # Initialize with some default rates (1 USD = X units of currency)
        # Rates are approximate and should be updated via FX feed
        self.rates = {
            'USD': Decimal('1.0'),      # US Dollar (base)
            'EUR': Decimal('0.85'),     # Euro
            'GBP': Decimal('0.73'),     # British Pound
            'JPY': Decimal('110.0'),    # Japanese Yen
            'CNY': Decimal('6.45'),     # Chinese Yuan
            'INR': Decimal('74.50'),    # Indian Rupee
            'BRL': Decimal('5.20'),     # Brazilian Real
            'CAD': Decimal('1.35'),     # Canadian Dollar
            'AUD': Decimal('1.45'),     # Australian Dollar
            'CHF': Decimal('0.92'),     # Swiss Franc
            'HKD': Decimal('7.78'),     # Hong Kong Dollar
            'SGD': Decimal('1.35'),     # Singapore Dollar
            'MXN': Decimal('20.0'),     # Mexican Peso
            'KRW': Decimal('1150.0'),   # South Korean Won
            'NZD': Decimal('1.48'),     # New Zealand Dollar
            'SEK': Decimal('8.50'),     # Swedish Krona
            'NOK': Decimal('8.80'),     # Norwegian Krone
            'DKK': Decimal('6.35'),     # Danish Krone
            'PLN': Decimal('3.90'),     # Polish Zloty
            'THB': Decimal('33.0'),     # Thai Baht
            'IDR': Decimal('14500.0'),  # Indonesian Rupiah
            'ZAR': Decimal('15.0'),     # South African Rand
            'RUB': Decimal('74.0'),     # Russian Ruble
            'TRY': Decimal('8.60')      # Turkish Lira
        }
        self.last_update = datetime.now()
        logger.info("Currency converter initialized with default rates for %d currencies", len(self.rates))
    
    def update_rates(self, fx_data: Dict) -> None:
        """Update FX rates from Kafka message"""
        try:
            # Extract rates from FX data
            new_rates = fx_data  # The rates are directly in the root object
            if not new_rates:
                logger.warning("Received empty rates data")
                return
            
            # Update rates
            for currency, rate in new_rates.items():
                if currency not in ['USD', 'timestamp']:  # Skip base currency and timestamp
                    self.rates[currency] = Decimal(str(rate))
            
            self.last_update = datetime.now()
            logger.info(f"Updated {len(new_rates) - 2} currency rates")  # -2 for USD and timestamp
            
        except Exception as e:
            logger.error(f"Error updating FX rates: {e}")
    
    def convert_to_usd(self, amount: Union[float, Decimal, str], from_currency: str) -> Tuple[float, float]:
        """Convert an amount from given currency to USD"""
        try:
            # Convert amount to Decimal if it isn't already
            if not isinstance(amount, Decimal):
                amount = Decimal(str(amount))
            
            if from_currency == 'USD':
                return float(amount), 1.0
            
            if from_currency not in self.rates:
                logger.warning(f"Unknown currency {from_currency}, using 1:1 rate")
                return float(amount), 1.0
            
            rate = self.rates[from_currency]
            usd_amount = amount / rate
            
            return float(usd_amount.quantize(Decimal('0.01'))), float(rate)
            
        except Exception as e:
            logger.error(f"Error converting {from_currency} to USD: {e}")
            return float(amount), 1.0
    
    def convert(self, amount: Union[float, Decimal, str], from_currency: str, to_currency: str) -> Tuple[float, float]:
        """Convert between any two currencies"""
        try:
            # Convert amount to Decimal if it isn't already
            if not isinstance(amount, Decimal):
                amount = Decimal(str(amount))
            
            if from_currency == to_currency:
                return float(amount), 1.0
            
            # First convert to USD if not already
            if from_currency != 'USD':
                usd_amount, _ = self.convert_to_usd(amount, from_currency)
                usd_amount = Decimal(str(usd_amount))
            else:
                usd_amount = amount
            
            # Then convert from USD to target currency
            if to_currency == 'USD':
                return float(usd_amount), float(self.rates.get(from_currency, Decimal('1.0')))
            
            if to_currency not in self.rates:
                logger.warning(f"Unknown target currency {to_currency}, using 1:1 rate")
                return float(usd_amount), 1.0
            
            final_amount = usd_amount * self.rates[to_currency]
            rate = self.rates[to_currency] / self.rates.get(from_currency, Decimal('1.0'))
            
            return float(final_amount.quantize(Decimal('0.01'))), float(rate)
            
        except Exception as e:
            logger.error(f"Error converting {from_currency} to {to_currency}: {e}")
            return float(amount), 1.0

# For testing
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test the converter
    converter = CurrencyConverter()
    
    # Test some conversions with different types
    test_amounts = [
        100,                    # int
        100.50,                # float
        Decimal('100.75'),     # Decimal
        '100.99'               # string
    ]
    
    from_curr = 'EUR'
    to_curr = 'USD'
    
    for amount in test_amounts:
        converted, rate = converter.convert(amount, from_curr, to_curr)
        logger.info(f"Converted {amount} ({type(amount)}) {from_curr} to {converted} {to_curr} (rate: {rate})")
    
    # Test rate updates
    new_rates = {
        'timestamp': datetime.now().isoformat(),
        'base_currency': 'USD',
        'rates': {
            'EUR': 0.82,
            'GBP': 0.72,
            'JPY': 109.5
        }
    }
    
    converter.update_rates(new_rates)
    
    # Test conversion with new rates
    amount = Decimal('100.00')
    converted, rate = converter.convert(amount, from_curr, to_curr)
    logger.info(f"After rate update: {amount} {from_curr} = {converted} {to_curr} (rate: {rate})")