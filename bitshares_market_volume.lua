requests = require('requests')

local queries = require('queries')
local functions = require('functions')
local config = require('config')

headers = {['Content-Type'] = 'application/json'}

markets = {} -- our table to store quote_id, base_id, quote_amount and base_amount

-- queries ES with all fill orders taker side in period of time
response = requests.get{config.es_direct_connection .. '_search?scroll=1m', data=queries.first_query(config.from, config.to), headers = headers}
json_body, error = response.json()

total = json_body.hits.total -- total fill orders in period
scroll_id = json_body._scroll_id -- We use scroll to query ES as it cant retrive by default more than 10000 results

-- parse first 1000 results from first query
functions.loop_fills(json_body.hits.hits)

-- use scroll api to get the rest of the hits until total == 0
while total > 0 do

    scroll_query = [[
    {
      "scroll" : "1m",
      "scroll_id" : "]] .. scroll_id .. [["
    }

    ]]
    response = requests.get{config.es_direct_connection .. '_search/scroll', data=scroll_query, headers = headers}
    json_body, error = response.json()

    -- load markets with data
    functions.loop_fills(json_body.hits.hits)
end

-- header csv
print("Market,",
    "Sum(Quote amounts),",
    "Sum(Base amounts),",
    "Quote/BTS ticker,",
    "Base/bts ticker,",
    "Volume in BTS,",
    "Trade count")

-- loop collected data in markets table
for k, v in pairs(markets) do

    -- get quote name
    if v.quote == "1.3.0" then
        quote_name = "BTS"
    else
        quote_query=queries.asset_query(v.quote)
        response = requests.get{config.es_direct_connection .. '_search', data=quote_query, headers = headers}
        json_body, error = response.json()
        quote_name = json_body.hits.hits[1]._source.symbol
        quote_precision = json_body.hits.hits[1]._source.precision
    end

    -- get base name
    if v.base == "1.3.0" then
        base_name = "BTS"
    else
        base_query=queries.asset_query(v.base)
        response = requests.get{config.es_direct_connection .. '_search', data=base_query, headers = headers}
        json_body, error = response.json()
        base_name = json_body.hits.hits[1]._source.symbol
        base_precision = json_body.hits.hits[1]._source.precision

    end

    -- get ticker data using quote
    response = requests.get{config.rest_bitshares_api .. 'get_ticker?base=BTS&quote=' .. quote_name .. '', headers = headers}
    json_body, error = response.json()

    if json_body.latest == nil then
        latest_using_quote = 0
    else
        latest_using_quote = json_body.latest
    end

    -- get ticker data using base
    response = requests.get{config.rest_bitshares_api .. 'get_ticker?base=BTS&quote=' .. base_name .. '', headers = headers}
    json_body, error = response.json()

    if json_body.latest == nil then
        latest_using_base = 0
    else
        latest_using_base = json_body.latest
    end

    -- calculate volume in bts as we can
    volume_in_bts = 0
    if tonumber(latest_using_quote) > 0 and  tonumber(latest_using_base) == 0 then
        volume_in_bts = (v.quote_amount/10^quote_precision)*latest_using_quote
    elseif tonumber(latest_using_base) > 0 and tonumber(latest_using_quote) == 0 then
        volume_in_bts = (v.base_amount/10^base_precision)*latest_using_base
    elseif tonumber(latest_using_base) == 0 and tonumber(latest_using_quote) == 0 then
        volume_in_bts = 0
    elseif tonumber(latest_using_base) > 0 and tonumber(latest_using_quote) > 0 then
        volume_in_bts = (v.quote_amount/10^quote_precision)*latest_using_quote
    end

    -- print csv
    print(quote_name .. "/" .. base_name .. ",",
        v.quote_amount .. ",",
        v.base_amount .. ",",
        latest_using_quote .. ",",
        latest_using_base .. ",",
        volume_in_bts .. ",",
        v.trades)
end
