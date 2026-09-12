import logging
from tastytrade import Session
import datetime
import asyncio
from gexfunctions import *
import httpx
import pandas
import os
from dotenv import load_dotenv

#This bot sells put spreads at the put wall when 0DTE net GEX(static open interest calculation) is positive, 
#spot price is over the put wall, and the Risk:Reward for the put spread at the put wall is 2:1 or less.
#It takes profits when the price of the put spread gets down to .05.

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
    ticker = 'SPY' #Ticker used for GEX calculation and selling put spreads
    num_expirations = 1 #The number of expirations you want to include in the GEX calculation. 1 is 0DTEs only.
    contracts_per_trade = 1 #This is the number of contracts you want to sell at the put wall.
    percentage_below = 2 #For GEX Calculation, this is the percentage below the put wall that you want to sell the put spread at.
    percentage_above = 2 #For GEX Calculation, this is the percentage above the put wall that you want to sell the put spread at.
    unit_of_risk_to_unit_of_reward_for_put_spread = 2 #i.e. 2:1 Risk to Reward ratio required to sell puts at the put wall
    strike_increment = 1.0 #Dollar or point difference between strikes for the ticker
    num_increments = 1 # Adjust this value to change how many strikes above/below you want to sell the put spread
    take_profit_price_of_put_spread = 0.03 #price that put spread has to get down to to take profits
    fees = 3 #The commission you pay for a one lot order of a single spread
    trade_taken_today = is_trade_taken_today("put_wall_seller_trades.csv")
    put_wall_contract_symbol = None
    lower_contract_symbol = None
    opening_order_id = None
    closing_order_id = None
    opening_price = None
    closing_price = None
    daily_profit = None

    while True:
        #Initial Trade
        if trade_taken_today == False and datetime.datetime.now().time() >= datetime.time(8, 45) and datetime.datetime.now().time() < datetime.time(12, 00):
            spot_price, total_gex, put_wall_strike, call_wall_strike, highest_abs_strike, highest_net_strike, highest_call_volume_strike, highest_put_volume_strike, highest_call_volume_strike_volume, highest_put_volume_strike_volume = await stream_gex(ticker, session, percentage_above, percentage_below, num_expirations)
            if check_for_put_wall_selling(spot_price, total_gex, put_wall_strike):
                result_bool, lower_contract_symbol, put_wall_contract_symbol, put_wall_strike_when_trade_was_taken = await get_put_spread_price_difference(put_wall_strike=put_wall_strike, num_increments=num_increments, strike_increment=strike_increment, ticker=ticker, session=session, rr_requirement=unit_of_risk_to_unit_of_reward_for_put_spread)
                if result_bool == True:
                    opening_order_id = paper_hypothesis_4_execution(put_wall_contract_symbol, lower_contract_symbol, API_KEY, API_SECRET, contracts_per_trade)
                    save_opening_id("put_wall_seller_order_ids.csv", opening_order_id)
                    log_trade_today("put_wall_seller_trades.csv")
                    trade_taken_today = True
                    trade_symbols = pandas.DataFrame([{
                    'date': datetime.datetime.now().date(),
                    'lower_contract_symbol': lower_contract_symbol,
                    'put_wall_contract_symbol': put_wall_contract_symbol,
                    'put_wall_strike_when_trade_was_taken': put_wall_strike_when_trade_was_taken
                    }])
                    trade_symbols.to_csv('put_wall_seller_trade_symbols.csv', mode='w', header=True, index=False)

        #Watch for Profit Taking
        if trade_taken_today == True and datetime.datetime.now().time() < datetime.time(15, 15) and datetime.datetime.now().time() >= datetime.time(8, 45):
            if put_wall_strike_when_trade_was_taken is None:
                put_wall_strike_when_trade_was_taken = get_put_wall_trade_symbols()
            profit_taking_bool, lower_contract_symbol, put_wall_contract_symbol = await check_put_spread_price(put_wall_strike_when_trade_was_taken, num_increments, strike_increment, session, take_profit_price_of_put_spread, ticker)
            if profit_taking_bool: 
                closing_order_id = paper_hypothesis_4_closing_position_take_profit(put_wall_contract_symbol, lower_contract_symbol, API_KEY, API_SECRET, contracts_per_trade, take_profit_price_of_put_spread)
                save_closing_id("put_wall_seller_order_ids.csv", closing_order_id)
                await asyncio.sleep(120)
                opening_order_id, closing_order_id = get_order_ids("put_wall_seller_order_ids.csv")
                opening_price = get_order_fill_price_alpaca(opening_order_id, API_KEY, API_SECRET)
                closing_price = get_order_fill_price_alpaca(closing_order_id, API_KEY, API_SECRET)
                daily_profit = ((opening_price - closing_price) * 100) - fees
                new_row = pandas.DataFrame([{
                    'date': datetime.datetime.now().date(),
                    'opening_price_of_put_spread': opening_price,
                    'closing_price_of_put_spread': closing_price,
                    'daily_profit': daily_profit
                }])

                new_row.to_csv('put_wall_seller_data.csv', mode='a', header=False, index=False)
                break

        #Close Position Before Market Close
        if trade_taken_today == True and datetime.datetime.now().time() >= datetime.time(14, 40) and datetime.datetime.now().time() < datetime.time(15, 15):
            if put_wall_strike_when_trade_was_taken is None:
                put_wall_strike_when_trade_was_taken = get_put_wall_trade_symbols()
                _, lower_contract_symbol, put_wall_contract_symbol = await check_put_spread_price(put_wall_strike_when_trade_was_taken, num_increments, strike_increment, session, take_profit_price_of_put_spread, ticker)
            closing_order_id = paper_hypothesis_4_closing_position(put_wall_contract_symbol, lower_contract_symbol, API_KEY, API_SECRET, contracts_per_trade)
            save_closing_id("put_wall_seller_order_ids.csv", closing_order_id)
            await asyncio.sleep(120)
            opening_order_id, closing_order_id = get_order_ids("put_wall_seller_order_ids.csv")
            opening_price = get_order_fill_price_alpaca(opening_order_id, API_KEY, API_SECRET)
            closing_price = get_order_fill_price_alpaca(closing_order_id, API_KEY, API_SECRET)
            daily_profit = ((opening_price - closing_price) * 100) - fees
            new_row = pandas.DataFrame([{
                'date': datetime.datetime.now().date(),
                'opening_price_of_put_spread': opening_price,
                'closing_price_of_put_spread': closing_price,
                'daily_profit': daily_profit
            }])

            new_row.to_csv('put_wall_seller_data.csv', mode='a', header=False, index=False)
            break
        
        #Stop if no trade taken before 12:00 CT
        if datetime.datetime.now().time() > datetime.time(12, 00) and trade_taken_today == False:
            print("It's after 12:00 PM and no trade was taken today.")
            break

        print('Ran Through\n')
        await asyncio.sleep(55)

if __name__ == "__main__":
    asyncio.run(main())