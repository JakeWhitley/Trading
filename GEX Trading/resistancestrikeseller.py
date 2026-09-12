import logging
from tastytrade import Session
import datetime
import asyncio
from gexfunctions import *
import httpx
import pandas
import os
from dotenv import load_dotenv

#This bot sells call spreads at the max of the call wall, highest absolute GEX strike,
#and highest net GEX strike(all taken using static OI GEX calculation). It checks to make sure that the spot price is below the max of the three
#strikes, then makes sure there is at 2:1 Risk:Reward or less for the call spread at the max of the three strikes.

async def main():
    load_dotenv()
    #tastytrade
    client_secret = os.getenv('tt_client_secret')
    refresh_token = os.getenv('tt_refresh_token')
    logging.getLogger('tastytrade').setLevel(logging.WARNING)
    session = Session(client_secret, refresh_token)
    session._client.timeout = httpx.Timeout(45.0, connect=30.0)

    #Alpaca
    API_KEY = os.getenv('ALPACA_API_KEY')
    API_SECRET = os.getenv('ALPACA_API_SECRET')

    #Inputs for trading logic
    ticker = 'SPY' #Ticker used for GEX calculation and selling call spreads
    num_expirations = 1 #The number of expirations you want to include in the GEX calculation. 1 is 0DTEs only.
    contracts_per_trade = 1 #This is the number of contracts you want to sell at the call wall.
    percentage_below = 2 #For GEX Calculation, this is the percentage below the call wall that you want to sell the call spread at.
    percentage_above = 2 #For GEX Calculation, this is the percentage above the call wall that you want to sell the call spread at.
    unit_of_risk_to_unit_of_reward_for_call_spread = 2 #i.e. 2:1 Risk to Reward ratio required to sell calls at the call wall
    strike_increment = 1.0 #Dollar or point difference between strikes for the ticker
    num_increments = 1 # Adjust this value to change how many strikes above/below you want to sell the call spread
    take_profit_price_of_call_spread = 0.03 #price that call spread has to get down to to take profits
    fees = 3 #The commission you pay for a one lot order of a single spread
    trade_taken_today = is_trade_taken_today("resistance_strike_seller_trades.csv")
    resistance_strike_contract_symbol = None
    upper_contract_symbol = None
    opening_order_id = None
    closing_order_id = None
    opening_price = None
    closing_price = None
    daily_profit = None
    max_resistance_strike_when_trade_was_taken = None

    while True:
        if trade_taken_today == False and datetime.datetime.now().time() >= datetime.time(8, 45) and datetime.datetime.now().time() < datetime.time(12, 00):
            spot_price, total_gex, put_wall_strike, call_wall_strike, highest_abs_strike, highest_net_strike, highest_call_volume_strike, highest_put_volume_strike, highest_call_volume_strike_volume, highest_put_volume_strike_volume = await stream_gex(ticker, session, percentage_above, percentage_below, num_expirations)
            resistance_bool, resistance_strike = check_for_resistance_strike_selling(spot_price, highest_abs_strike, call_wall_strike, highest_net_strike)
            if resistance_bool:
                result_bool, upper_contract_symbol, resistance_strike_contract_symbol, max_resistance_strike_when_trade_was_taken = await get_call_spread_price_difference(resistance_strike, num_increments, strike_increment, ticker, session, unit_of_risk_to_unit_of_reward_for_call_spread)
                if result_bool:
                    opening_order_id = paper_call_volume_strike_execution(resistance_strike_contract_symbol, upper_contract_symbol, API_KEY, API_SECRET, contracts_per_trade)
                    save_opening_id("resistance_strike_seller_order_ids.csv", opening_order_id)
                    log_trade_today("resistance_strike_seller_trades.csv")
                    trade_taken_today = True
                    trade_symbols = pandas.DataFrame([{
                        'date': datetime.datetime.now().date(),
                        'upper_contract_symbol': upper_contract_symbol,
                        'resistance_strike_contract_symbol': resistance_strike_contract_symbol,
                        'max_resistance_strike_when_trade_was_taken': max_resistance_strike_when_trade_was_taken
                        }])
                    trade_symbols.to_csv('resistance_strike_seller_trade_symbols.csv', mode='w', header=True, index=False)

        #Watch for profit taking
        if trade_taken_today == True and datetime.datetime.now().time() < datetime.time(15, 15) and datetime.datetime.now().time() >= datetime.time(8, 45):
            if max_resistance_strike_when_trade_was_taken is None:
                max_resistance_strike_when_trade_was_taken = get_resistance_strike_trade_symbols()
            profit_taking_bool, upper_contract_symbol, resistance_strike_contract_symbol = await check_call_spread_price(max_resistance_strike_when_trade_was_taken, num_increments, strike_increment, session, take_profit_price_of_call_spread, ticker)
            if profit_taking_bool:
                closing_order_id = paper_call_volume_closing_position_take_profit(resistance_strike_contract_symbol, upper_contract_symbol, API_KEY, API_SECRET, contracts_per_trade, take_profit_price_of_call_spread)
                save_closing_id("resistance_strike_seller_order_ids.csv", closing_order_id)
                await asyncio.sleep(120)
                opening_order_id, closing_order_id = get_order_ids("resistance_strike_seller_order_ids.csv")
                opening_price = get_order_fill_price_alpaca(opening_order_id, API_KEY, API_SECRET)
                closing_price = get_order_fill_price_alpaca(closing_order_id, API_KEY, API_SECRET)
                daily_profit = ((opening_price - closing_price) * 100) - fees
                new_row = pandas.DataFrame([{
                    'date': datetime.datetime.now().date(),
                    'opening_price_of_call_spread': opening_price,
                    'closing_price_of_call_spread': closing_price,
                    'daily_profit': daily_profit
                }])

                new_row.to_csv('resistance_strike_seller_data.csv', mode='a', header=False, index=False)
                break

        #Close Position Before Market Close
        if trade_taken_today == True and datetime.datetime.now().time() >= datetime.time(14, 40) and datetime.datetime.now().time() < datetime.time(15, 15):
            if max_resistance_strike_when_trade_was_taken is None:
                max_resistance_strike_when_trade_was_taken = get_resistance_strike_trade_symbols()
                _, upper_contract_symbol, resistance_strike_contract_symbol = await check_call_spread_price(max_resistance_strike_when_trade_was_taken, num_increments, strike_increment, session, take_profit_price_of_call_spread, ticker)
            closing_order_id = paper_call_volume_closing_position(resistance_strike_contract_symbol, upper_contract_symbol, API_KEY, API_SECRET, contracts_per_trade)
            save_closing_id("resistance_strike_seller_order_ids.csv", closing_order_id)
            await asyncio.sleep(120)
            opening_order_id, closing_order_id = get_order_ids("resistance_strike_seller_order_ids.csv")
            opening_price = get_order_fill_price_alpaca(opening_order_id, API_KEY, API_SECRET)
            closing_price = get_order_fill_price_alpaca(closing_order_id, API_KEY, API_SECRET)
            daily_profit = ((opening_price - closing_price) * 100) - fees
            new_row = pandas.DataFrame([{
                'date': datetime.datetime.now().date(),
                'opening_price_of_call_spread': opening_price,
                'closing_price_of_call_spread': closing_price,
                'daily_profit': daily_profit
            }])

            new_row.to_csv('resistance_strike_seller_data.csv', mode='a', header=False, index=False)
            break

        #Stop if no trade taken before 12:00 CT
        if datetime.datetime.now().time() > datetime.time(12, 00) and trade_taken_today == False:
            print("It's after 12:00 PM and no trade was taken today.")
            break

        print('Ran Through\n')
        await asyncio.sleep(55)

if __name__ == "__main__":
    asyncio.run(main())