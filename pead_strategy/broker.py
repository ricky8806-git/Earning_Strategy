# pead_strategy/broker.py
from alpaca.trading.client   import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums    import OrderSide, TimeInForce
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY


def _get_client():
    return TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)


def get_account():
    """Return dict with cash, portfolio_value, and positions."""
    client = _get_client()
    acct   = client.get_account()
    return {
        'cash':            float(acct.cash),
        'portfolio_value': float(acct.portfolio_value),
        'positions':       get_current_positions(client=client),
    }


def get_current_positions(client=None):
    """Return {symbol: shares} for all open positions."""
    if client is None:
        client = _get_client()
    return {p.symbol: float(p.qty) for p in client.get_all_positions()}


def place_order(symbol, side, notional, client=None):
    """Place a market order by notional dollar amount."""
    if client is None:
        client = _get_client()
    req = MarketOrderRequest(
        symbol        = symbol,
        notional      = round(notional, 2),
        side          = OrderSide.BUY if side == 'buy' else OrderSide.SELL,
        time_in_force = TimeInForce.DAY,
    )
    return client.submit_order(req)


def close_position(symbol, client=None):
    """Close entire position in a symbol. Swallows errors (position may already be closed)."""
    if client is None:
        client = _get_client()
    try:
        client.close_position(symbol)
    except Exception as e:
        print(f"[broker] Warning: could not close {symbol}: {e}")


def rebalance(target_weights, portfolio_value, client=None):
    """
    Bring the portfolio in line with target_weights.

    1. Close positions not in target_weights.
    2. For each target position, buy/sell the difference between
       current market value and desired notional.
    """
    if client is None:
        client = _get_client()

    current = {p.symbol: float(p.market_value) for p in client.get_all_positions()}

    # Close positions that are no longer in the target
    for symbol in list(current.keys()):
        if symbol not in target_weights:
            close_position(symbol, client)

    # Adjust target positions
    for symbol, weight in target_weights.items():
        target_notional  = portfolio_value * weight
        current_notional = current.get(symbol, 0.0)
        diff             = target_notional - current_notional

        if diff > 10:            # Need to buy more
            place_order(symbol, 'buy',  diff, client)
        elif diff < -10:         # Need to sell some
            place_order(symbol, 'sell', abs(diff), client)
