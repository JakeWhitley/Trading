from aiohttp import streamer
import numpy as np
import pandas as pd
import tastytrade as tt
import asyncio
import datetime
import requests
import json
import logging
from tastytrade import Session
from tastytrade import Account
from tastytrade import DXLinkStreamer
from tastytrade.dxfeed import Greeks
from tastytrade.instruments import get_option_chain
from tastytrade.dxfeed import Summary
from tastytrade.dxfeed import Quote
from tastytrade.dxfeed import Trade
import heapq
import os
from datetime import date
import csv

def get_resistance_strike_trade_symbols(csv_filename='resistance_strike_seller_trade_symbols.csv'):
    # Read CSV (headers will automatically load from line 1)
    df = pd.read_csv(csv_filename)
    df = df.fillna('')
    # Grab the most recent row (last row)
    latest_row = df.iloc[-1]
    
    resistance = latest_row['max_resistance_strike_when_trade_was_taken']
    
    return float(resistance)

def get_call_volume_trade_symbols(csv_filename='highest_call_volume_strike_seller_trade_symbols.csv'):
    # Read CSV (headers will automatically load from line 1)
    df = pd.read_csv(csv_filename)
    df = df.fillna('')
    # Grab the most recent row (last row)
    latest_row = df.iloc[-1]
    
    call_volume_strike = latest_row['highest_call_volume_strike_when_trade_was_taken']
    
    return float(call_volume_strike)

def get_put_volume_trade_symbols(csv_filename='highest_put_volume_strike_seller_trade_symbols.csv'):
    # Read CSV (headers will automatically load from line 1)
    df = pd.read_csv(csv_filename)
    df = df.fillna('')
    # Grab the most recent row (last row)
    latest_row = df.iloc[-1]
    
    highest_put_volume_strike_when_trade_was_taken = latest_row['highest_put_volume_strike_when_trade_was_taken']
    
    return float(highest_put_volume_strike_when_trade_was_taken)

def get_put_wall_trade_symbols(csv_filename='put_wall_seller_trade_symbols.csv'):
    # Read CSV (headers will automatically load from line 1)
    df = pd.read_csv(csv_filename, dtype=str)
    df = df.fillna('')
    # Grab the most recent row (last row)
    latest_row = df.iloc[-1]
    
    put_wall_strike_when_trade_was_taken = latest_row['put_wall_strike_when_trade_was_taken']
    
    return float(put_wall_strike_when_trade_was_taken)

def get_order_ids(file_path: str) -> tuple[str | None, str | None]:
    """
    Reads the CSV and returns (opening_id, closing_id) for today's trade.
    Returns (None, None) if no orders are logged yet or if the file doesn't exist.
    """
    if not os.path.exists(file_path):
        return None, None
        
    today_str = str(date.today())
    opening_id = None
    closing_id = None
    
    with open(file_path, mode="r", newline="") as f:
        for row in csv.reader(f):
            # Verify valid row structure and make sure it's from today
            if len(row) >= 3 and row[0] == today_str:
                if row[1] == "OPEN":
                    opening_id = str(row[2])
                elif row[1] == "CLOSE":
                    closing_id = str(row[2])
                    
    return opening_id, closing_id

def save_opening_id(file_path: str, opening_id: str) -> None:
    """Appends today's date and the opening order ID to the CSV."""
    today_str = str(date.today())
    opening_id = str(opening_id)
    
    with open(file_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([today_str, "OPEN", opening_id])
    print(f"Saved opening order ID {opening_id} to {file_path}")

def save_closing_id(file_path: str, closing_id: str) -> None:
    """Appends today's date and the closing order ID to the CSV."""
    today_str = str(date.today())
    closing_id = str(closing_id)
    
    with open(file_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([today_str, "CLOSE", closing_id])
    print(f"Saved closing order ID {closing_id} to {file_path}")

def is_trade_taken_today(file_path: str) -> bool:
    if not os.path.exists(file_path):
        return False
        
    today_str = str(date.today())
    
    with open(file_path, mode="r", newline="") as f:
        rows = [row for row in csv.reader(f) if row] # filter empty lines
        if not rows:
            return False
            
        last_row = rows[-1] # [-1] grabs the last line of the file!
        return last_row[0] == today_str

def log_trade_today(file_path: str) -> None:
    """Appends today's date string to the CSV file."""
    today_str = str(date.today())
    
    with open(file_path, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([today_str])
        
    print(f"Successfully logged trade for {today_str} to {file_path}")
    
async def fetch_contract_data(streamer, contract):
    # Subscribe to both Greeks and Summary on the same streamer session
    await streamer.subscribe(Greeks, contract.streamer_symbol)
    await streamer.subscribe(Summary, contract.streamer_symbol)
    await streamer.subscribe(Trade, contract.streamer_symbol)

    # Get the events

    greeks = await streamer.get_event(Greeks)
    summary = await streamer.get_event(Summary)
    trade = await streamer.get_event(Trade)

    # Return the data

    return contract, greeks, summary, trade

def check_for_put_wall_selling(spot, total_gamma, put_wall):
  if spot > put_wall and total_gamma > 0:
    print('Conditions for Put Selling Met')
    return True
    
  else:
    print('Conditions for Put Selling Not Met')
    return False

def check_for_highest_call_volume_strike_selling(spot, highest_call_volume_strike_volume, highest_call_volume_strike):
  if spot < highest_call_volume_strike and highest_call_volume_strike_volume >= 100000:
    print('Conditions for Call Selling Met')
    return True
  else:
    print('Conditions for Call Selling Not Met')
    return False

def check_for_highest_put_volume_strike_selling(spot, total_gamma, highest_put_volume_strike_volume, highest_put_volume_strike):
  if spot > highest_put_volume_strike and total_gamma > -5000000000 and highest_put_volume_strike_volume >= 100000:
    print('Conditions for Put Selling Met')
    return True
  else:
    print('Conditions for Put Selling Not Met')
    return False

def check_for_resistance_strike_selling(spot, highest_abs_gex_strike, call_wall_strike, highest_net_gex_strike):
   resistance_strike = max(highest_abs_gex_strike, call_wall_strike, highest_net_gex_strike)
   if spot < resistance_strike:
       print('Conditions for Resistance Strike Call Spread Selling Met')
       return True, float(resistance_strike)
   else:
       print('Conditions for Resistance Strike Call Spread Selling Not Met')
       return False, float(resistance_strike)

async def get_put_spread_price_difference(
    put_wall_strike,
    num_increments,
    strike_increment,
    ticker,
    session,
    rr_requirement,
    num_expirations=1
):
    #for selling one below the put wall
    #put_wall_strike = float(put_wall_strike) - (num_increments * strike_increment)

    # Calculate the strike price for the contract below the put wall
    lower_strike = float(put_wall_strike) - (num_increments * strike_increment)

    chain = await get_option_chain(session, ticker)
    #earliest_expiration = min(chain.keys())
    #filtered_contracts = [contract for contract in chain[earliest_expiration] if min_strike <= contract.strike_price <= max_strike]
    X = num_expirations  
    
    # Sort the expirations and grab the nearest X
    nearest_expirations = sorted(chain.keys())[:X]
    
    # 2. Filter and collect contracts across ALL of those X expirations
    filtered_contracts = []
    for expiration in nearest_expirations:
        expiration_contracts = [
            contract for contract in chain[expiration] 
            if lower_strike <= contract.strike_price <= put_wall_strike
        ]
        filtered_contracts.extend(expiration_contracts)
    
    # Find the contracts at put_wall_strike and lower_strike
    put_wall_contract = None
    lower_strike_contract = None

    for contract in filtered_contracts:
        if contract.option_type == 'P' and contract.strike_price == put_wall_strike:
            put_wall_contract = contract
        if contract.option_type == 'P' and contract.strike_price == lower_strike:
            lower_strike_contract = contract
        if put_wall_contract and lower_strike_contract: # Optimization: stop early if both found
            break

    if not put_wall_contract:
        return None
    if not lower_strike_contract:
        return None

    # Fetch prices for both contracts
    put_wall_mid_price = None
    lower_strike_mid_price = None

    async with DXLinkStreamer(session) as streamer:
        # Get price for put_wall_contract
        await streamer.subscribe(Quote, put_wall_contract.streamer_symbol)
        put_wall_quote = await streamer.get_event(Quote)
        put_wall_mid_price = (put_wall_quote.bid_price + put_wall_quote.ask_price) / 2

    async with DXLinkStreamer(session) as streamer:
        # Get price for lower_strike_contract
        await streamer.subscribe(Quote, lower_strike_contract.streamer_symbol)
        lower_strike_quote = await streamer.get_event(Quote)
        lower_strike_mid_price = (lower_strike_quote.bid_price + lower_strike_quote.ask_price) / 2

    if put_wall_mid_price is not None and lower_strike_mid_price is not None:
        price_difference = float(put_wall_mid_price) - float(lower_strike_mid_price)
        max_loss = (num_increments * strike_increment) - price_difference

    if price_difference >= (max_loss/rr_requirement):
      print(f'Put Spread Price: {price_difference}, Max Loss: {max_loss}, Meets Risk to Reward Requirement: {rr_requirement}:1')
      return True, str(lower_strike_contract.symbol.replace(' ', '')), str(put_wall_contract.symbol.replace(' ', '')), float(put_wall_strike)
    else:
      print(f'Put Spread Price: {price_difference}, Max Loss: {max_loss}, Does NOT Meet Risk to Reward Requirement: {rr_requirement}:1')
      return False, str(lower_strike_contract.symbol.replace(' ', '')), str(put_wall_contract.symbol.replace(' ', '')), float(put_wall_strike)

async def get_call_spread_price_difference(
    call_wall_strike,
    num_increments,
    strike_increment,
    ticker,
    session,
    rr_requirement,
    num_expirations=1
):
    
    #Set the call_wall_strike one higher than it actually is
    #call_wall_strike = float(call_wall_strike) + strike_increment

    #Calculate the strike price for the contract above the call wall
    upper_strike = float(call_wall_strike) + (num_increments * strike_increment)

    chain = await get_option_chain(session, ticker)
    #earliest_expiration = min(chain.keys())
    #filtered_contracts = [contract for contract in chain[earliest_expiration] if min_strike <= contract.strike_price <= max_strike]
    X = num_expirations  

    # Sort the expirations and grab the nearest X
    nearest_expirations = sorted(chain.keys())[:X]

    # 2. Filter and collect contracts across ALL of those X expirations
    filtered_contracts = []
    for expiration in nearest_expirations:
        expiration_contracts = [
            contract for contract in chain[expiration] 
            if call_wall_strike <= contract.strike_price <= upper_strike
        ]
        filtered_contracts.extend(expiration_contracts)

    call_wall_contract = None
    upper_strike_contract = None

    for contract in filtered_contracts:
        if contract.option_type == 'C' and contract.strike_price == float(call_wall_strike):
            call_wall_contract = contract
        if contract.option_type == 'C' and contract.strike_price == upper_strike:
            upper_strike_contract = contract
        if call_wall_contract and upper_strike_contract: # Optimization: stop early if both found
            break

    if not call_wall_contract:
        return None
    if not upper_strike_contract:
        return None

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, call_wall_contract.streamer_symbol)
        call_wall_quote = await streamer.get_event(Quote)
        call_wall_mid_price = (call_wall_quote.bid_price + call_wall_quote.ask_price) / 2

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, upper_strike_contract.streamer_symbol)
        upper_strike_quote = await streamer.get_event(Quote)
        upper_strike_mid_price = (upper_strike_quote.bid_price + upper_strike_quote.ask_price) / 2


    if call_wall_mid_price is not None and upper_strike_mid_price is not None:
        price_difference = float(call_wall_mid_price) - float(upper_strike_mid_price)
        max_loss = (num_increments * strike_increment) - price_difference

    if price_difference >= (max_loss/rr_requirement):
      print(f'Call Spread Price: {price_difference}, Max Loss: {max_loss}, Meets Risk to Reward Requirement: {rr_requirement}:1')
      return True, str(upper_strike_contract.symbol.replace(' ', '')), str(call_wall_contract.symbol.replace(' ', '')), float(call_wall_strike)
    else:
      print(f'Call Spread Price: {price_difference}, Max Loss: {max_loss}, Does NOT Meet Risk to Reward Requirement: {rr_requirement}:1')
      return False, str(upper_strike_contract.symbol.replace(' ', '')), str(call_wall_contract.symbol.replace(' ', '')), float(call_wall_strike)

def paper_hypothesis_4_execution(put_wall_contract_symbol, lower_contract_symbol, API_KEY, API_SECRET, num_contracts=1):
    ORDER_URL = "https://paper-api.alpaca.markets/v2/orders"

    quantity = str(num_contracts)
    payload = {
        "order_class": "mleg",
        "qty": quantity,
        "type": "limit",
        "limit_price": "-.30",
        "time_in_force": "day",
        "legs": [
        {
            "symbol": put_wall_contract_symbol,
            "ratio_qty": "1",
            "side": "sell",
            "position_intent": "sell_to_open"
        },
        {
        "symbol": lower_contract_symbol,
        "ratio_qty": "1",
        "side": "buy",
        "position_intent": "buy_to_open"
        }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET
    }

    response = requests.post(ORDER_URL, json=payload, headers=headers)

    if not response.ok:
        print(f"Order failed with status {response.status_code}: {response.text}")
        response.raise_for_status()

    order_data = response.json()
    order_id = order_data["id"]

    print(f"Executed put spread. Order ID: {order_id}")
    return str(order_id)

def paper_call_volume_strike_execution(call_wall_contract_symbol, upper_contract_symbol, API_KEY, API_SECRET, num_contracts=1):
    ORDER_URL = "https://paper-api.alpaca.markets/v2/orders"

    quantity = str(num_contracts)
    payload = {
        "order_class": "mleg",
        "qty": quantity,
        "type": "limit",
        "limit_price": "-.30",
        "time_in_force": "day",
        "legs": [
        {
            "symbol": call_wall_contract_symbol,
            "ratio_qty": "1",
            "side": "sell",
            "position_intent": "sell_to_open"
        },
        {
        "symbol": upper_contract_symbol,
        "ratio_qty": "1",
        "side": "buy",
        "position_intent": "buy_to_open"
        }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET
    }

    response = requests.post(ORDER_URL, json=payload, headers=headers)

    if not response.ok:
        print(f"Order failed with status {response.status_code}: {response.text}")
        response.raise_for_status()

    order_data = response.json()
    order_id = order_data["id"]

    print(f"Executed call spread. Order ID: {order_id}")
    return str(order_id)

def paper_hypothesis_4_closing_position_take_profit(put_wall_contract_symbol, lower_contract_symbol, API_KEY, API_SECRET, num_contracts=1, take_profit_value=0.05):
    ORDER_URL = "https://paper-api.alpaca.markets/v2/orders"

    quantity = str(num_contracts)
    limit_price = str(take_profit_value)
    payload = {
        "order_class": "mleg",
        "qty": quantity,
        "type": "limit",
        "limit_price": limit_price,
        "time_in_force": "day",
        "legs": [
        {
            "symbol": put_wall_contract_symbol,
            "ratio_qty": "1",
            "side": "buy",
            "position_intent": "buy_to_close"
        },
        {
        "symbol": lower_contract_symbol,
        "ratio_qty": "1",
        "side": "sell",
        "position_intent": "sell_to_close"
        }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET
    }

    response = requests.post(ORDER_URL, json=payload, headers=headers)
    
    if not response.ok:
        print(f"Order failed with status {response.status_code}: {response.text}")
        response.raise_for_status()

    order_data = response.json()
    order_id = order_data["id"]

    print(f"Closed put spread at take profit: {limit_price}. Order ID: {order_id}")
    return str(order_id)

def paper_call_volume_closing_position_take_profit(call_wall_contract_symbol, upper_contract_symbol, API_KEY, API_SECRET, num_contracts=1, take_profit_value=0.05):
    ORDER_URL = "https://paper-api.alpaca.markets/v2/orders"

    quantity = str(num_contracts)
    limit_price = str(take_profit_value)
    payload = {
        "order_class": "mleg",
        "qty": quantity,
        "type": "limit",
        "limit_price": limit_price,
        "time_in_force": "day",
        "legs": [
        {
            "symbol": call_wall_contract_symbol,
            "ratio_qty": "1",
            "side": "buy",
            "position_intent": "buy_to_close"
        },
        {
        "symbol": upper_contract_symbol,
        "ratio_qty": "1",
        "side": "sell",
        "position_intent": "sell_to_close"
        }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET
    }

    response = requests.post(ORDER_URL, json=payload, headers=headers)
    
    if not response.ok:
        print(f"Order failed with status {response.status_code}: {response.text}")
        response.raise_for_status()

    order_data = response.json()
    order_id = order_data["id"]

    print(f"Closed call spread at take profit: {limit_price}. Order ID: {order_id}")
    return str(order_id)

def paper_hypothesis_4_closing_position(put_wall_contract_symbol, lower_contract_symbol, API_KEY, API_SECRET, num_contracts=1):
    ORDER_URL = "https://paper-api.alpaca.markets/v2/orders"

    quantity = str(num_contracts)
    payload = {
        "order_class": "mleg",
        "qty": quantity,
        "type": "market",
        "time_in_force": "day",
        "legs": [
        {
            "symbol": put_wall_contract_symbol,
            "ratio_qty": "1",
            "side": "buy",
            "position_intent": "buy_to_close"
        },
        {
        "symbol": lower_contract_symbol,
        "ratio_qty": "1",
        "side": "sell",
        "position_intent": "sell_to_close"
        }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET
    }

    response = requests.post(ORDER_URL, json=payload, headers=headers)
    
    if not response.ok:
        print(f"Order failed with status {response.status_code}: {response.text}")
        response.raise_for_status()

    order_data = response.json()
    order_id = order_data["id"]

    print(f"Closed put spread at market price. Order ID: {order_id}")
    return str(order_id)

def paper_call_volume_closing_position(call_wall_contract_symbol, upper_contract_symbol, API_KEY, API_SECRET, num_contracts=1):
    ORDER_URL = "https://paper-api.alpaca.markets/v2/orders"

    quantity = str(num_contracts)
    payload = {
        "order_class": "mleg",
        "qty": quantity,
        "type": "market",
        "time_in_force": "day",
        "legs": [
        {
            "symbol": call_wall_contract_symbol,
            "ratio_qty": "1",
            "side": "buy",
            "position_intent": "buy_to_close"
        },
        {
        "symbol": upper_contract_symbol,
        "ratio_qty": "1",
        "side": "sell",
        "position_intent": "sell_to_close"
        }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET
    }

    response = requests.post(ORDER_URL, json=payload, headers=headers)
    
    if not response.ok:
        print(f"Order failed with status {response.status_code}: {response.text}")
        response.raise_for_status()

    order_data = response.json()
    order_id = order_data["id"]

    print(f"Closed call spread at market price. Order ID: {order_id}")
    return str(order_id)

def get_order_fill_price_alpaca(order_id, api_key, secret_key):
    url = "https://paper-api.alpaca.markets/v2/orders/" + order_id + "?nested=true"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key
    }

    response = requests.get(url, headers=headers)

    return abs(float(response.json()['filled_avg_price']))
    print(response.text)

def check_for_correct_open_credit_spread(positions, short_contract_symbol, long_contract_symbol):
  if short_contract_symbol and long_contract_symbol in positions:
    return True
  else:
    return False

def get_open_positions(API_KEY, API_SECRET):
  url = "https://paper-api.alpaca.markets/v2/positions"

  headers = {
      "accept": "application/json",
      "APCA-API-KEY-ID": API_KEY,
      "APCA-API-SECRET-KEY": API_SECRET
  }

  response = requests.get(url, headers=headers)

  return response.text

async def get_spot_price(ticker, session):
    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, ticker)
        quote = await streamer.get_event(Quote)

    return float((quote.bid_price + quote.ask_price) / 2)

async def check_put_spread_price(
    put_wall_strike,
    num_increments,
    strike_increment,
    session,
    take_profit_price,
    ticker,
    num_expirations=1
):
    # Calculate the strike price for the contract below the put wall
    lower_strike = float(put_wall_strike) - (num_increments * strike_increment)

    chain = await get_option_chain(session, ticker)
    #earliest_expiration = min(chain.keys())
    #filtered_contracts = [contract for contract in chain[earliest_expiration] if min_strike <= contract.strike_price <= max_strike]
    X = num_expirations  
    
    # Sort the expirations and grab the nearest X
    nearest_expirations = sorted(chain.keys())[:X]
    
    # 2. Filter and collect contracts across ALL of those X expirations
    filtered_contracts = []
    for expiration in nearest_expirations:
        expiration_contracts = [
            contract for contract in chain[expiration] 
            if lower_strike <= contract.strike_price <= put_wall_strike
        ]
        filtered_contracts.extend(expiration_contracts)

    # Find the contracts at put_wall_strike and lower_strike
    put_wall_contract = None
    lower_strike_contract = None

    for contract in filtered_contracts:
        if contract.option_type == 'P' and contract.strike_price == put_wall_strike:
            put_wall_contract = contract
            tt_put_wall_contract_symbol = contract
        if contract.option_type == 'P' and contract.strike_price == lower_strike:
            lower_strike_contract = contract
            tt_lower_contract_symbol = contract
        if put_wall_contract and lower_strike_contract: # Optimization: stop early if both found
            break

    if not put_wall_contract:
        return None
    if not lower_strike_contract:
        return None

    # Fetch prices for both contracts
    put_wall_mid_price = None
    lower_strike_mid_price = None

    async with DXLinkStreamer(session) as streamer:
        # Get price for put_wall_contract
        await streamer.subscribe(Quote, put_wall_contract.streamer_symbol)
        put_wall_quote = await streamer.get_event(Quote)
        put_wall_mid_price = (put_wall_quote.bid_price + put_wall_quote.ask_price) / 2

    async with DXLinkStreamer(session) as streamer:
        # Get price for lower_strike_contract
        await streamer.subscribe(Quote, lower_strike_contract.streamer_symbol)
        lower_strike_quote = await streamer.get_event(Quote)
        lower_strike_mid_price = (lower_strike_quote.bid_price + lower_strike_quote.ask_price) / 2

    if put_wall_mid_price is not None and lower_strike_mid_price is not None:
        price_difference = float(put_wall_mid_price) - float(lower_strike_mid_price)
        max_loss = (num_increments * strike_increment) - price_difference

    if price_difference > take_profit_price:
      print(f"Put spread has NOT reached the take profit price of {take_profit_price}. Current spread: {price_difference}")
      return False, str(lower_strike_contract.symbol.replace(' ', '')), str(put_wall_contract.symbol.replace(' ', ''))

    else:
      print(f"Put spread has reached the take profit price of {take_profit_price}. Current spread: {price_difference}")
      return True, str(lower_strike_contract.symbol.replace(' ', '')), str(put_wall_contract.symbol.replace(' ', ''))

async def check_call_spread_price(
    call_wall_strike,
    num_increments,
    strike_increment,
    session,
    take_profit_price, 
    ticker,
    num_expirations=1
):
    #Set the call_wall_strike one higher than it actually is
    #call_wall_strike = float(call_wall_strike) + strike_increment

    #Calculate the strike price for the contract above the call wall
    upper_strike = float(call_wall_strike) + (num_increments * strike_increment)

    chain = await get_option_chain(session, ticker)
    #earliest_expiration = min(chain.keys())
    #filtered_contracts = [contract for contract in chain[earliest_expiration] if min_strike <= contract.strike_price <= max_strike]
    X = num_expirations  

    # Sort the expirations and grab the nearest X
    nearest_expirations = sorted(chain.keys())[:X]

    # 2. Filter and collect contracts across ALL of those X expirations
    filtered_contracts = []
    for expiration in nearest_expirations:
        expiration_contracts = [
            contract for contract in chain[expiration] 
            if call_wall_strike <= contract.strike_price <= upper_strike
        ]
        filtered_contracts.extend(expiration_contracts)

    call_wall_contract = None
    upper_strike_contract = None

    for contract in filtered_contracts:
        if contract.option_type == 'C' and contract.strike_price == call_wall_strike:
            call_wall_contract = contract
        if contract.option_type == 'C' and contract.strike_price == upper_strike:
            upper_strike_contract = contract
        if call_wall_contract and upper_strike_contract: # Optimization: stop early if both found
            break

    if not call_wall_contract:
        return None
    if not upper_strike_contract:
        return None

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, call_wall_contract.streamer_symbol)
        call_wall_quote = await streamer.get_event(Quote)
        call_wall_mid_price = (call_wall_quote.bid_price + call_wall_quote.ask_price) / 2

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Quote, upper_strike_contract.streamer_symbol)
        upper_strike_quote = await streamer.get_event(Quote)
        upper_strike_mid_price = (upper_strike_quote.bid_price + upper_strike_quote.ask_price) / 2


    if call_wall_mid_price is not None and upper_strike_mid_price is not None:
        price_difference = float(call_wall_mid_price) - float(upper_strike_mid_price)
        max_loss = (num_increments * strike_increment) - price_difference

    if price_difference > take_profit_price:
      print(f"Call spread has NOT reached the take profit price of {take_profit_price}. Current spread: {price_difference}")
      return False, str(upper_strike_contract.symbol.replace(' ', '')), str(call_wall_contract.symbol.replace(' ', ''))
    
    else:
      print(f"Call spread has reached the take profit price of {take_profit_price}. Current spread: {price_difference}")
      return True, str(upper_strike_contract.symbol.replace(' ', '')), str(call_wall_contract.symbol.replace(' ', ''))

async def stream_gex(ticker, session, percentage_above, percentage_below, num_expirations):

        below_multiple = 1 - (percentage_below / 100)
        above_multiple = 1 + (percentage_above / 100)

        spot_price = await get_spot_price(ticker, session)

        min_strike = spot_price * below_multiple
        max_strike = spot_price * above_multiple

        total_gex = 0
        net_gex= {}
        abs_gex = {}
        call_gex_map = {}
        put_gex_map = {}
        call_volume_map = {}
        put_volume_map = {}

        chain = await get_option_chain(session, ticker)
        #earliest_expiration = min(chain.keys())
        #filtered_contracts = [contract for contract in chain[earliest_expiration] if min_strike <= contract.strike_price <= max_strike]
        X = num_expirations  

        # Sort the expirations and grab the nearest X
        nearest_expirations = sorted(chain.keys())[:X]

        # 2. Filter and collect contracts across ALL of those X expirations
        filtered_contracts = []
        for expiration in nearest_expirations:
            expiration_contracts = [
                contract for contract in chain[expiration] 
                if min_strike <= contract.strike_price <= max_strike
            ]
            filtered_contracts.extend(expiration_contracts)

        async with DXLinkStreamer(session) as streamer:
            tasks = [fetch_contract_data(streamer, contract) for contract in filtered_contracts]
            results = await asyncio.gather(*tasks)

        for contract, greeks, summary, trade in results:
            gamma = greeks.gamma
            oi = summary.open_interest
            strike = contract.strike_price
            contract_type = contract.option_type
            daily_volume = trade.day_volume

            contract_gex = float(gamma) * 100 * oi * (spot_price**2) * .01

            if contract_type == 'P':
                contract_gex *= -1

            total_gex += contract_gex

            if contract_type == 'C':
                call_gex_map[strike] = call_gex_map.get(strike, 0.0) + contract_gex
                call_volume_map[strike] = call_volume_map.get(strike, 0) + daily_volume if daily_volume is not None else call_volume_map.get(strike, 0)

            if contract_type == 'P':
                put_gex_map[strike] = put_gex_map.get(strike, 0.0) + contract_gex
                put_volume_map[strike] = put_volume_map.get(strike, 0) + daily_volume if daily_volume is not None else put_volume_map.get(strike, 0)

            net_gex[strike] = net_gex.get(strike, 0.0) + contract_gex
            abs_gex[strike] = abs_gex.get(strike, 0.0) + abs(contract_gex)

        highest_net_strike = max(net_gex.keys(), key=lambda k: net_gex[k]) if net_gex else None
        highest_net_val = net_gex[highest_net_strike] if highest_net_strike is not None else 0.0

        highest_abs_strike = max(abs_gex.keys(), key=lambda k: abs(abs_gex[k])) if abs_gex else None
        highest_abs_val = abs(abs_gex[highest_abs_strike]) if highest_abs_strike is not None else 0.0

        call_wall_strike = max(call_gex_map.keys(), key=lambda k: call_gex_map[k]) if call_gex_map else None
        call_wall_val = call_gex_map[call_wall_strike] if call_wall_strike is not None else 0.0

        put_wall_strike = min(put_gex_map.keys(), key=lambda k: put_gex_map[k]) if put_gex_map else None
        put_wall_val = put_gex_map[put_wall_strike] if put_wall_strike is not None else 0.0
        #If you want to spit out the highest volume strikes that are the lowest(puts) or highest(calls) out of the 3 strikes with the most volume:
        #top_3_call_volume_strikes = heapq.nlargest(3, call_volume_map, key=call_volume_map.get)
        #top_3_put_volume_strikes = heapq.nlargest(3, put_volume_map, key=put_volume_map.get)
        #highest_call_vol_strike = max(top_3_call_volume_strikes) if top_3_call_volume_strikes else None
        #highest_put_vol_strike = min(top_3_put_volume_strikes) if top_3_put_volume_strikes else None

        highest_call_vol_strike = max(call_volume_map.keys(), key=lambda k: call_volume_map[k], default=0.0)
        highest_put_vol_strike = max(put_volume_map.keys(), key=lambda k: put_volume_map[k], default=0.0)

        print(f'Current Spot Price of {ticker}: {spot_price}')
        print(f"Total GEX: {total_gex:.2f}")
        print(f"Highest Net GEX Strike: {highest_net_strike} with value {highest_net_val:.2f}")
        print(f"Highest Absolute GEX Strike: {highest_abs_strike} with value {highest_abs_val:.2f}")
        print(f"Call Wall Strike: {call_wall_strike} with value {call_wall_val:.2f}")
        print(f"Put Wall Strike: {put_wall_strike} with value {put_wall_val:.2f}")
        print(f"Highest Call Volume Strike: {highest_call_vol_strike} with volume {call_volume_map.get(highest_call_vol_strike, 0)}")
        print(f"Highest Put Volume Strike: {highest_put_vol_strike} with volume {put_volume_map.get(highest_put_vol_strike, 0)}")

        spot_price = float(spot_price)
        total_gex = float(total_gex)
        highest_net_strike = float(highest_net_strike) if highest_net_strike is not None else 'NA'
        highest_net_val = float(highest_net_val) 
        highest_abs_strike = float(highest_abs_strike) if highest_abs_strike is not None else 'NA'
        highest_abs_val = float(highest_abs_val)
        call_wall_strike = float(call_wall_strike) if call_wall_strike is not None else 'NA'
        call_wall_val = float(call_wall_val)
        put_wall_strike = float(put_wall_strike) if put_wall_strike is not None else 'NA'
        put_wall_val = float(put_wall_val)
        highest_call_vol_strike = float(highest_call_vol_strike)
        highest_put_vol_strike = float(highest_put_vol_strike)
        highest_call_volume_strike_volume = float(call_volume_map.get(highest_call_vol_strike, 0))
        highest_put_volume_strike_volume = float(put_volume_map.get(highest_put_vol_strike, 0))

        return spot_price, total_gex, put_wall_strike, call_wall_strike, highest_abs_strike, highest_net_strike, highest_call_vol_strike, highest_put_vol_strike, highest_call_volume_strike_volume, highest_put_volume_strike_volume