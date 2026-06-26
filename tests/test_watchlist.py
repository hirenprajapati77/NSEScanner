import unittest
from datetime import datetime, timedelta
from watchlist import get_watchlist_status

class TestWatchlist(unittest.TestCase):
    def setUp(self):
        self.bull_stock = {
            'symbol': 'BULL.NS',
            'added_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'manually_closed': 0
        }
        self.bear_stock = {
            'symbol': 'BEAR.NS',
            'added_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'manually_closed': 0
        }

    def test_bull_triggered(self):
        # Entry at 100, currently 105 (triggered)
        res = get_watchlist_status(self.bull_stock, 100, 95, 110, 105, "Bull")
        self.assertEqual(res['status'], 'TRIGGERED')
        self.assertEqual(res['current_rr'], 1.0) # (105-100) / (100-95) = 5/5 = 1.0
        
    def test_bull_waiting(self):
        # Entry at 100, currently 98 (waiting)
        res = get_watchlist_status(self.bull_stock, 100, 95, 110, 98, "Bull")
        self.assertEqual(res['status'], 'WAITING')
        self.assertEqual(res['current_rr'], -0.4) # (98-100) / 5 = -0.4
        
    def test_bear_triggered(self):
        # Entry at 100, sl 105, target 90. Current 95 (price dropped, so triggered in profit)
        res = get_watchlist_status(self.bear_stock, 100, 105, 90, 95, "Bear")
        self.assertEqual(res['status'], 'TRIGGERED')
        self.assertEqual(res['current_rr'], 1.0) # (100-95) / (105-100) = 5/5 = 1.0
        
    def test_bear_waiting(self):
        # Entry at 100, sl 105. Current 102 (price above entry, waiting)
        res = get_watchlist_status(self.bear_stock, 100, 105, 90, 102, "Bear")
        self.assertEqual(res['status'], 'WAITING')
        self.assertEqual(res['current_rr'], -0.4) # (100-102) / (105-100) = -2/5 = -0.4
        
    def test_bear_dist_to_entry(self):
        # Entry 100, current 102 -> distance is (102-100)/100 * 100 = 2.0%
        res = get_watchlist_status(self.bear_stock, 100, 105, 90, 102, "Bear")
        self.assertEqual(res['dist_to_entry'], 2.0)

if __name__ == '__main__':
    unittest.main()
