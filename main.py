import configparser
import asyncio
from ib_insync import *
import pandas as pd
import datetime

class IBKRConnector:
    def __init__(self, config_file):
        self.config = self.read_config(config_file)
        self.ib = IB()
        self.s1_contract = None
        self.s1_bars = None
        self.s1_ha_bars = None
        self.s2_contract = None
        self.s2_bars = None
        self.s2_ha_bars = None
        self.oca_group = None  # Initialize the OCA group variable
        self.entry_price = None
        self.upper_pnl = float(self.config['upper_pnl'])
        self.lower_pnl = float(self.config['lower_pnl'])
    
    def read_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config['general']

    async def connect(self):
        await self.ib.connectAsync('127.0.0.1', 7497, clientId=14)

    async def get_historical_data(self, contract, resolution, duration):
        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            durationStr=duration,
            barSizeSetting=resolution.strip('"'),
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1,
            keepUpToDate=True
        )
        return bars

    async def start(self):
        await self.connect()
        # S1 data (MNQ continuous futures)
        self.s1_contract = Contract(symbol='MNQ', exchange='CME', secType='CONTFUT', multiplier=2)
        await self.ib.qualifyContractsAsync(self.s1_contract)
        self.s1_bars = await self.get_historical_data(self.s1_contract, self.config['resolution'], '2 D')
        self.s1_ha_bars = self.compute_heikin_ashi(self.s1_bars)
        self.s1_ha_bars['ema'] = self.compute_ema(self.s1_ha_bars['close'], int(self.config['ema_period']))
        self.s1_bars.updateEvent += self.on_s1_bar_update

        # S2 data (VIX index)
        self.s2_contract = Index(symbol='VIX', exchange='CBOE', currency='USD')
        await self.ib.qualifyContractsAsync(self.s2_contract)
        self.s2_bars = await self.get_historical_data(self.s2_contract, self.config['vix_resolution'], '2 D')
        self.s2_ha_bars = self.compute_heikin_ashi(self.s2_bars)
        self.s2_ha_bars['ema'] = self.compute_ema(self.s2_ha_bars['close'], int(self.config['vix_ema_period']))
        self.s2_bars.updateEvent += self.on_s2_bar_update
        self.ib.execDetailsEvent += self.on_order_filled

        await self.execute()

        while True:
            await asyncio.sleep(0.1)

    def on_order_filled(self, trade, fill):
        if trade.order.orderRef == "entry":
            filled_price = fill.execution.price
            self.entry_price = filled_price
            stop_loss_distance = float(self.config['fixed_stoploss']) * 0.25
            self.oca_group = f"oca_{trade.order.orderId}"
            if trade.order.action == 'BUY':
                stop_loss_price = filled_price - stop_loss_distance
                stop_order = StopOrder('SELL', trade.order.totalQuantity, stop_loss_price, orderRef="fixed_stop_loss", ocaGroup=self.oca_group)
            elif trade.order.action == 'SELL':
                stop_loss_price = filled_price + stop_loss_distance
                stop_order = StopOrder('BUY', trade.order.totalQuantity, stop_loss_price, orderRef="fixed_stop_loss", ocaGroup=self.oca_group)

            stop_order.outsideRth = True  # Allow stop order outside RTH
            self.ib.placeOrder(self.s1_contract, stop_order)

    def has_open_position(self):
        positions = self.ib.positions()
        for position in positions:
            if position.contract.symbol == self.s1_contract.symbol:
                return True
        return False

    def has_pending_trailing_stop_order(self):
        pending_orders = self.ib.openTrades()
        for trade in pending_orders:
            if trade.order.orderRef == "trailing_stop_loss":
                return True
        return False
    
    def exit_s1_and_cancel_orders(self):
        # Cancel all pending orders for S1
        pending_orders = self.ib.openTrades()
        for trade in pending_orders:
            if trade.contract.symbol == self.s1_contract.symbol:
                self.ib.cancelOrder(trade.order)

        # Close the open position for S1
        positions = self.ib.positions()
        for position in positions:
            if position.contract.symbol == self.s1_contract.symbol:
                if position.position > 0:
                    order = MarketOrder('SELL', position.position)
                    self.ib.placeOrder(self.s1_contract, order)
                elif position.position < 0:
                    order = MarketOrder('BUY', abs(position.position))
                    self.ib.placeOrder(self.s1_contract, order)

    async def get_daily_pnl(self):
        account_summary = await self.ib.accountSummaryAsync()
        for summary in account_summary:
            if summary.tag == 'RealizedPnL' and summary.currency == 'USD':
                return float(summary.value)
        return 0.0

    async def is_pnl_within_limits(self):
        daily_pnl = await self.get_daily_pnl()
        return self.lower_pnl <= daily_pnl <= self.upper_pnl

    async def entry(self):
        if self.has_open_position() or self.has_pending_trailing_stop_order():
            return
        
        if not await self.is_pnl_within_limits():
            return

        condition = self.config.get('condition')

        if condition == '1':
            await self.condition1_entry()
        elif condition == '2':
            await self.condition2_entry()

    async def condition1_entry(self):
        # Check the second-to-last bar
        if self.s1_ha_bars is not None and len(self.s1_ha_bars) > 1:
            second_last_bar = self.s1_ha_bars.iloc[-2]
            second_last_ema = self.s1_ha_bars['ema'].iloc[-2]

            if second_last_bar['close'] > second_last_ema:
                order = MarketOrder('BUY', int(self.config['size']), orderRef="entry")
                order.outsideRth = True  # Allow order outside RTH
                self.ib.placeOrder(self.s1_contract, order)
            elif second_last_bar['close'] < second_last_ema:
                order = MarketOrder('SELL', int(self.config['size']), orderRef="entry")
                order.outsideRth = True  # Allow order outside RTH
                self.ib.placeOrder(self.s1_contract, order)

    async def condition2_entry(self):
        # Check the second-to-last bar for S1 and the last bar for S2
        if self.s1_ha_bars is not None and len(self.s1_ha_bars) > 1 and self.s2_ha_bars is not None and len(self.s2_ha_bars) > 0:
            second_last_bar_s1 = self.s1_ha_bars.iloc[-2]
            last_bar_s2 = self.s2_ha_bars.iloc[-1]
            second_last_ema_s1 = self.s1_ha_bars['ema'].iloc[-2]
            last_ema_s2 = self.s2_ha_bars['ema'].iloc[-1]

            if (second_last_bar_s1['close'] > second_last_bar_s1['open'] and second_last_bar_s1['close'] > second_last_ema_s1 
                and last_bar_s2['close'] < last_ema_s2):
                order = MarketOrder('BUY', int(self.config['size']), orderRef="entry")
                order.outsideRth = True  # Allow order outside RTH
                self.ib.placeOrder(self.s1_contract, order)
            elif (second_last_bar_s1['close'] < second_last_bar_s1['open'] and second_last_bar_s1['close'] < second_last_ema_s1 
                  and last_bar_s2['close'] > last_ema_s2):
                order = MarketOrder('SELL', int(self.config['size']), orderRef="entry")
                order.outsideRth = True  # Allow order outside RTH
                self.ib.placeOrder(self.s1_contract, order)

    def get_position_size(self):
        positions = self.ib.positions()
        for position in positions:
            if position.contract.symbol == self.s1_contract.symbol:
                return position.position
        return 0
    
    async def exit(self):
        position_size = self.get_position_size()
        condition = self.config.get('condition')

        if condition == '1' and position_size != 0:
            await self.condition1_exit(position_size)
        elif condition == '2' and position_size != 0:
            await self.condition2_exit(position_size)

    async def condition1_exit(self, position_size):
        # Check the second-to-last bar for the opposite condition
        if self.s1_ha_bars is not None and len(self.s1_ha_bars) > 1:
            second_last_bar = self.s1_ha_bars.iloc[-2]
            second_last_ema = self.s1_ha_bars['ema'].iloc[-2]

            # Opposite condition: close < EMA if long, close > EMA if short
            if position_size > 0 and second_last_bar['close'] < second_last_ema:
                self.exit_s1_and_cancel_orders()
            elif position_size < 0 and second_last_bar['close'] > second_last_ema:
                self.exit_s1_and_cancel_orders()

    async def condition2_exit(self, position_size):
        # Check the second-to-last bar for S1
        if self.s1_ha_bars is not None and len(self.s1_ha_bars) > 1:
            second_last_bar_s1 = self.s1_ha_bars.iloc[-2]
            second_last_ema_s1 = self.s1_ha_bars['ema'].iloc[-2]

            if position_size > 0 and second_last_bar_s1['close'] < second_last_ema_s1 and second_last_bar_s1['close'] < second_last_bar_s1['open']:
                self.exit_s1_and_cancel_orders()
            elif position_size < 0 and second_last_bar_s1['close'] > second_last_ema_s1 and second_last_bar_s1['close'] > second_last_bar_s1['open']:
                self.exit_s1_and_cancel_orders()

    async def manage_positions(self):
        position_size = self.get_position_size()
        if position_size != 0 and not self.has_pending_trailing_stop_order():
            last_bar = self.s1_bars[-1] if self.s1_bars else None
            if last_bar is None:
                return

            if self.entry_price is None:
                trades = self.ib.trades()
                if trades:
                    last_trade = trades[-1]
                    if last_trade.contract.symbol == self.s1_contract.symbol:
                        self.entry_price = last_trade.orderStatus.avgFillPrice
                    else:
                        return
                else:
                    return

            trailing_stop_loss_distance = float(self.config['trailing_stoploss']) * 0.25
            trailing_stop_loss_trigger_distance = float(self.config['trailing_stoploss_trigger']) * 0.25

            try:
                if position_size > 0:  # Long position
                    distance = last_bar.high - self.entry_price
                    if distance >= trailing_stop_loss_trigger_distance:
                        trailing_stop_order = Order(
                            action='SELL',
                            totalQuantity=position_size,
                            orderType='TRAIL',
                            auxPrice=trailing_stop_loss_distance,
                            trailStopPrice=last_bar.close - trailing_stop_loss_distance,
                            orderRef="trailing_stop_loss",
                            ocaGroup=self.oca_group
                        )
                        trailing_stop_order.outsideRth = True  # Allow trailing stop order outside RTH
                        self.ib.placeOrder(self.s1_contract, trailing_stop_order)
                elif position_size < 0:  # Short position
                    distance = self.entry_price - last_bar.low
                    if distance >= trailing_stop_loss_trigger_distance:
                        trailing_stop_order = Order(
                            action='BUY',
                            totalQuantity=abs(position_size),
                            orderType='TRAIL',
                            auxPrice=trailing_stop_loss_distance,
                            trailStopPrice=last_bar.close + trailing_stop_loss_distance,
                            orderRef="trailing_stop_loss",
                            ocaGroup=self.oca_group
                        )
                        trailing_stop_order.outsideRth = True  # Allow trailing stop order outside RTH
                        self.ib.placeOrder(self.s1_contract, trailing_stop_order)
            except Exception:
                pass

    async def execute(self):
        await self.exit()
        await asyncio.sleep(3)
        await self.entry()

    def on_s1_bar_update(self, bars, hasNewBar):
        self.s1_bars = bars
        df = util.df(bars)
        self.s1_ha_bars = self.compute_heikin_ashi(bars)
        self.s1_ha_bars['ema'] = self.compute_ema(df['close'], int(self.config['ema_period']))
        
        if hasNewBar:
            asyncio.create_task(self.execute())
        try:
            asyncio.create_task(self.manage_positions())
        except Exception:
            pass

    def on_s2_bar_update(self, bars, hasNewBar):
        self.s2_bars = bars
        self.s2_ha_bars = self.compute_heikin_ashi(bars)
        self.s2_ha_bars['ema'] = self.compute_ema(self.s2_ha_bars['close'], int(self.config['vix_ema_period']))

    def compute_heikin_ashi(self, bars):
        df = util.df(bars)
        ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4

        ha_open = ha_close.copy()
        ha_high = ha_close.copy()
        ha_low = ha_close.copy()

        for i in range(1, len(df)):
            ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2
            ha_high.iloc[i] = max(df['high'].iloc[i], ha_open.iloc[i], ha_close.iloc[i])
            ha_low.iloc[i] = min(df['low'].iloc[i], ha_open.iloc[i], ha_close.iloc[i])

        ha_df = pd.DataFrame({
            'open': ha_open,
            'high': ha_high,
            'low': ha_low,
            'close': ha_close
        })

        return ha_df

    def compute_ema(self, prices, period):
        alpha = 2 / (period + 1)
        ema = prices.copy()
        ema.iloc[0] = prices.iloc[0]  # Initialize EMA to the first price
        for i in range(1, len(prices)):
            ema.iloc[i] = prices.iloc[i] * alpha + ema.iloc[i - 1] * (1 - alpha)
        return ema

# Usage
config_file = 'cfg.ini'
connector = IBKRConnector(config_file)

# Run the connector
asyncio.run(connector.start())
