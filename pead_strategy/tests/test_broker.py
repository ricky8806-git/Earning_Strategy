# pead_strategy/tests/test_broker.py
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from unittest.mock import MagicMock, patch


def _mock_position(symbol, market_value):
    p = MagicMock()
    p.symbol       = symbol
    p.market_value = str(market_value)
    p.qty          = '10'
    return p


def _mock_account(cash='10000', portfolio_value='100000'):
    a = MagicMock()
    a.cash            = cash
    a.portfolio_value = portfolio_value
    return a


def test_get_account_returns_floats():
    mock_client = MagicMock()
    mock_client.get_account.return_value       = _mock_account('15000', '100000')
    mock_client.get_all_positions.return_value = []

    with patch('broker._get_client', return_value=mock_client):
        from broker import get_account
        result = get_account()

    assert result['cash']            == pytest.approx(15000.0)
    assert result['portfolio_value'] == pytest.approx(100000.0)
    assert result['positions']       == {}


def test_get_current_positions_maps_symbols():
    mock_client = MagicMock()
    mock_client.get_all_positions.return_value = [
        _mock_position('AAPL', 7000),
        _mock_position('SPY',  93000),
    ]

    with patch('broker._get_client', return_value=mock_client):
        from broker import get_current_positions
        result = get_current_positions(client=mock_client)

    assert set(result.keys()) == {'AAPL', 'SPY'}


def test_place_order_buy_submits_correct_side():
    from alpaca.trading.enums import OrderSide
    mock_client = MagicMock()

    with patch('broker._get_client', return_value=mock_client):
        from broker import place_order
        place_order('AAPL', 'buy', 5000.0, client=mock_client)

    mock_client.submit_order.assert_called_once()
    req = mock_client.submit_order.call_args[0][0]
    assert req.side    == OrderSide.BUY
    assert float(req.notional) == pytest.approx(5000.0)
    assert req.symbol  == 'AAPL'


def test_place_order_sell_submits_correct_side():
    from alpaca.trading.enums import OrderSide
    mock_client = MagicMock()

    with patch('broker._get_client', return_value=mock_client):
        from broker import place_order
        place_order('AAPL', 'sell', 3000.0, client=mock_client)

    req = mock_client.submit_order.call_args[0][0]
    assert req.side == OrderSide.SELL


def test_close_position_calls_alpaca():
    mock_client = MagicMock()

    with patch('broker._get_client', return_value=mock_client):
        from broker import close_position
        close_position('AAPL', client=mock_client)

    mock_client.close_position.assert_called_once_with('AAPL')


def test_rebalance_closes_positions_not_in_target():
    mock_client = MagicMock()
    mock_client.get_all_positions.return_value = [_mock_position('AAPL', 7000)]

    with patch('broker._get_client', return_value=mock_client):
        from broker import rebalance
        rebalance({'SPY': 1.0}, 100_000, client=mock_client)

    mock_client.close_position.assert_called_once_with('AAPL')


def test_rebalance_buys_new_target_position():
    mock_client = MagicMock()
    mock_client.get_all_positions.return_value = []  # No existing positions

    with patch('broker._get_client', return_value=mock_client):
        from broker import rebalance
        rebalance({'SPY': 0.93, 'AAPL': 0.07}, 100_000, client=mock_client)

    # Should have placed buy orders for both SPY and AAPL
    calls = mock_client.submit_order.call_args_list
    symbols_ordered = [c[0][0].symbol for c in calls]
    assert 'SPY'  in symbols_ordered
    assert 'AAPL' in symbols_ordered


def test_rebalance_skips_tiny_diff():
    """Differences under $10 should not trigger an order."""
    mock_client = MagicMock()
    # AAPL already at $7000, target is also ~$7000 (7% of $100k)
    mock_client.get_all_positions.return_value = [_mock_position('AAPL', 7000)]

    with patch('broker._get_client', return_value=mock_client):
        from broker import rebalance
        rebalance({'AAPL': 0.07}, 100_000, client=mock_client)

    # Diff = 7000 - 7000 = 0, no order
    mock_client.submit_order.assert_not_called()
