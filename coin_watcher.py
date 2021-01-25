#!/usr/bin/python
################
# Price aggregaitor and notifier

import collections
import logging
import logging.handlers
import os
import shutil
import gzip
import sys

import asyncio
import aiohttp
import aiosqlite
import uvloop


API_ENDPOINT = "https://api.coingecko.com/api/v3/coins/markets" \
               "?vs_currency=usd&" \
               "order=market_cap_desc&" \
               "per_page=100&" \
               "page=1&" \
               "sparkline=false&" \
               "price_change_percentage=24h"

class State:
  currencies = {}  # name: SizedDeque([ json ... ])
  last_update = 0
  

class Settings:
  push_notification_webhook = "https://somehost.com/"
  push_notification_webhook_method = "get"
  push_notification_webhook_kw = {"body": "yaayy", "user-agent":"yas"}

  update_interval = 30

###
# Custom timed rotator
# + Gunzip Compressed
# + Timed
# + Auto Deleted
class LogRotator(logging.handlers.TimedRotatingFileHandler):
  def doRollover(self):
    if self.stream:
      self.stream.close()

    if self.backupCount > 0:
      for i in range(self.backupCount - 1, 0, -1):
        sfn = "%s.%d.gz" % (self.baseFilename, i)
        dfn = "%s.%d.gz" % (self.baseFilename, i + 1)
        if os.path.exists(sfn):
          if os.path.exists(dfn):
            os.remove(dfn)
          os.rename(sfn, dfn)

      dfn = self.baseFilename + ".1.gz"
      if os.path.exists(dfn):
        os.remove(dfn)

      with open(self.baseFilename, 'rb') as f_in, gzip.open(dfn, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    self.mode = 'w'
    self.stream = self._open()

handler = LogRotator(
  f"{sys.argv[0]}.log",
  when='midnight',
  backupCount=10
)
handler.suffix = "%Y-%m-%d"

logging.basicConfig(
  handlers=(handler,), # Allow GZIP Timed Handle to write to disk
  level=logging.INFO,
  format="%(asctime)s:%(levelname)s:%(message)s"
)

QUEUE_SIZE = 200
class SizedDeque(collections.deque):
  def __init__(self, size):
    self._set_size = size
    super().__init__()
  
  def clamp(self, set_size):
    self._get_size = set_size
    
  @property
  def clamp_size(self):
    return self._get_size
  
  def append(self, object):
    if len(self) >= self._set_size:
      del self[self._set_size-1:] # dude this is so clean
    super().appendleft(object)


def fmt_percent(percent, colorize=True):
  RESET = "\033[m"

  if abs(percent) >= .001:
    data = f"%{abs(percent):.1}"
  else:
    data = f"%{abs(percent):.2}"

  if percent == .0:
    data = "%0.00"
    if colorize:
      data = f"\033[1;90m{data}{RESET}"
    return data
  
  if percent < .0:
    data = "+"+data
    if colorize:
      data = f"\033[1;32m{data}{RESET}"
    
  else:
    data = "-"+data
    if colorize:
      data = f"\033[1;31m{data}{RESET}"
  
  return data

async def hit():
  async with aiohttp.ClientSession() as session:
    async with session.get(API_ENDPOINT) as response:
      return await response.json()
    

async def update_db(db, map, update_list):
  for name in update_list:
      latest = map[name][0]
      
      await db.execute(''' INSERT OR IGNORE INTO Coins(name, symbol) VALUES(?, ?);
      ''', (latest['name'], latest['symbol']))
      
      coin_id = await db.execute(''' select ID from Coins where name = ? and symbol = ?;
      ''', (latest['name'], latest['symbol']))
      coin_id = await coin_id.fetchone()
      
      await db.execute(''' insert into PriceIndex(
          price_per_coin,
          coin,
          volume,
          market_cap
      ) VALUES(?, ?, ?, ?);
      ''', (float(latest['current_price']), coin_id[0], latest['total_volume'], latest['market_cap']))

      # idx_id = await db.execute('''SELECT last_insert_rowid();''')
      # idx_id = await idx_id.fetchone()
      #
      # await db.execute(''' insert into BlobIndex(ID, coin_id, json_payload) values (?, ?, ?);
      # ''', (int(idx_id[0]), coin_id[0], str(latest)))
  await db.commit()


def update_state(map, response):
  '''
  :param map: Dict[str] = [response_data, ...]
  :param response: [Dict[...]]
  :return: None
  '''
  update_map = [] # [name...]
  
  for section in response:
    name = section["name"]
    price = section["current_price"]
    
    if not name in map:
      map[name] = SizedDeque(QUEUE_SIZE)
     
    if len(map[name]) > 0 and not map[name][0]["current_price"] == price:
      map[name].append(section)
      update_map.append(name)
      
    elif len(map[name]) == 0:
      map[name].append(section)
      update_map.append(name)
  
  return update_map

async def main():
  
  database = await aiosqlite.connect("history.db") # "history.db"
  with open("schema.sql", "r") as fd:
    for statement in fd.read().split(';'):
      try:
        await database.execute(statement.strip())
      except Exception as e:
        print(e, statement)
        exit(1)
  await database.commit()
  
  
  while True:
    new_data = await hit()
    updates = update_state(State.currencies, new_data)
    
    for name in updates:
      if len(State.currencies[name]) > 1:
          prior = price_diff_percent(State.currencies[name][0]["current_price"], State.currencies[name][1]["current_price"])
          logging.info(f"{name}: ${State.currencies[name][0]['current_price']} ({fmt_percent(prior)})")
    
    await update_db(database, State.currencies, updates)
    await asyncio.sleep(Settings.update_interval)

def price_diff_percent(prior, latest):
  diff = latest - prior
  result = diff/latest*100
  return result

def seek_by_closest_ts(coin_queue, wants_ts):
  '''
  
  This function returns the index of closest timestamp requested (`wants_ts`) inside of `coin_queue`
  
  NOTE: This function seeks a collection of `[(price, timestamp)]`
  where the order of the deque is LIFO (last in - first out).
  Further explaining, the newest price should be appended to the left hand side.
  
  :param coin_queue: [(float, float)]
  :param wants_ts: float
  :return  None|int
  '''
  idx = 0
  
  # we suppose this works because
  # it should be a greater value than the input `wants_ts`
  # and if not, we just break the loop on the first iteration
  bottom = coin_queue[0][1]
  
  # since theres a chance `coin_queue` could be the length of 0
  # or `wants_ts` is greater than the first option
  # we want to specify when returning our index
  # if the index is actually zero
  actually_zero = False

  for (i, (price, ts)) in enumerate(coin_queue):
    # We can actually skip the first iteration since
    # bottom is already assigned as the first iteration
    if i == 0:
      continue
    
    elif wants_ts == ts:
      return i

    elif bottom >= wants_ts-ts > 0: # try to get as close to zero as we can
      bottom = wants_ts-ts
      idx = i
      actually_zero = True

    else: # taking advantage of the LIFO so we only iterate `i` times
      actually_zero = True
      break
  
  if actually_zero and len(coin_queue) > 0:
    return idx
  
  return None

def start():
  loop = uvloop.new_event_loop()
  loop.run_until_complete(main())

if __name__ == '__main__':
  start()
  #test()
