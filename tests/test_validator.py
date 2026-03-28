"""
Tests for data validator
"""

import unittest
from datetime import datetime
from telecom_anomaly.validation.validator import DataValidator


class TestDataValidator(unittest.TestCase):
    
    def setUp(self):
        self.validator = DataValidator()
    
    def test_validate_msisdn_valid(self):
        self.assertTrue(self.validator.validate_msisdn('233123456789'))
        self.assertTrue(self.validator.validate_msisdn('MTN'))
    
    def test_validate_msisdn_invalid(self):
        self.assertFalse(self.validator.validate_msisdn(''))
        self.assertFalse(self.validator.validate_msisdn('12345'))
        self.assertFalse(self.validator.validate_msisdn('23312345'))  # Too short
    
    def test_validate_duration(self):
        self.assertEqual(self.validator.validate_duration('120'), 120.0)
        self.assertEqual(self.validator.validate_duration('-10'), 0.0)
        self.assertEqual(self.validator.validate_duration(''), 0.0)
        self.assertEqual(self.validator.validate_duration(None), 0.0)
    
    def test_validate_coordinates(self):
        lat, lon = self.validator.validate_coordinates('5.5', '-0.2')
        self.assertEqual(lat, 5.5)
        self.assertEqual(lon, -0.2)
        
        lat, lon = self.validator.validate_coordinates('', '')
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        
        lat, lon = self.validator.validate_coordinates('100', '200')  # Invalid
        self.assertIsNone(lat)
        self.assertIsNone(lon)
    
    def test_parse_datetime(self):
        dt = self.validator.parse_datetime('2024-01-01', '12:30:00')
        self.assertIsNotNone(dt)
        self.assertEqual(dt.year, 2024)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 1)
        
        dt = self.validator.parse_datetime('NULL', 'NULL')
        self.assertIsNone(dt)
    
    def test_is_night_call(self):
        self.assertTrue(self.validator.is_night_call('23:30:00', 23, 5))
        self.assertTrue(self.validator.is_night_call('02:00:00', 23, 5))
        self.assertFalse(self.validator.is_night_call('14:00:00', 23, 5))
        self.assertFalse(self.validator.is_night_call('', 23, 5))


if __name__ == '__main__':
    unittest.main()