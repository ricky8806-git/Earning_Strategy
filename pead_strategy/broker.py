# pead_strategy/broker.py
from alpaca.trading.client   import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums    import OrderSide, TimeInForce
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, REBALANCE_TOLERANCE


def _get_client():
    return TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True,
                         url_override=ALPACA_BASE_URL)


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
    2. For each target position, only trade if the current weight deviates
       from the target by more than REBALANCE_TOLERANCE (default ±2%).
       e.g. a 7% target won't trigger an order unless the position drifts
       below 5% or above 9% of portfolio value.
    """
    if client is None:
        client = _get_client()

    current = {p.symbol: float(p.market_value) for p in client.get_all_positions()}

    # Close positions that are no longer in the target
    for symbol in list(current.keys()):
        if symbol not in target_weights:
            close_position(symbol, client)

    # Adjust target positions only when drift exceeds the tolerance band
    for symbol, weight in target_weights.items():
        target_notional  = portfolio_value * weight
        current_notional = current.get(symbol, 0.0)
        current_weight   = current_notional / portfolio_value if portfolio_value else 0.0
        drift            = current_weight - weight

        if drift < -REBALANCE_TOLERANCE:      # Below lower band — buy up to target
            place_order(symbol, 'buy',  target_notional - current_notional, client)
        elif drift > REBALANCE_TOLERANCE:     # Above upper band — trim back to target
            place_order(symbol, 'sell', current_notional - target_notional, client)
