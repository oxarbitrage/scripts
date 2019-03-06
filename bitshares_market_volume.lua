
-- Utility function to print a recursive table keys and values
function DeepPrint (e)
    -- if e is a table, we should iterate over its elements
    if type(e) == "table" then
        for k,v in pairs(e) do -- for every element in the table
            print(k)
            DeepPrint(v)       -- recursively repeat the same procedure
        end
    else -- if not, we can just print it
        print(e)
    end
end

-- loop throw results in initial query and in scroll, save data to markets table
function loop_fills(hits)

    for k, v in pairs(hits) do

        total = total - 1
        --print("first: " .. total)

        new = true;

        if k > 1 then

            for k2, v2 in pairs(markets) do
                if v2.quote == v._source.operation_history.op_object.pays.asset_id and
                        v2.base == v._source.operation_history.op_object.receives.asset_id then

                    markets[k2].quote_amount = markets[k2].quote_amount + v._source.operation_history.op_object.pays.amount
                    markets[k2].base_amount = markets[k2].base_amount + v._source.operation_history.op_object.receives.amount
                    new = false

                    break
                end
            end
        end

        if new then
            markets[k] = {
                quote = v._source.operation_history.op_object.pays.asset_id,
                base = v._source.operation_history.op_object.receives.asset_id,
                quote_amount = v._source.operation_history.op_object.pays.amount,
                base_amount = v._source.operation_history.op_object.receives.amount
            }
        end
    end
end


requests = require('requests')

local queries = require('queries')

headers = {['Content-Type'] = 'application/json'}

response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search?scroll=1m', data=queries.first_query, headers = headers}

json_body, error = response.json()

markets = {}

total = json_body.hits.total
scroll_id = json_body._scroll_id
-- print(total)

loop_fills(json_body.hits.hits)

while total > 0 do

    scroll_query = [[
    {
      "scroll" : "1m",
      "scroll_id" : "]] .. scroll_id .. [["
    }

    ]]
    response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search/scroll', data=scroll_query, headers = headers}

    json_body, error = response.json()

    loop_fills(json_body.hits.hits)

end

-- DeepPrint(markets)

-- need to make this more effcient as i am querying each record to just get an asset symbol
for k, v in pairs(markets) do

    quote_query=queries.asset_query(v.quote)
    response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search', data=quote_query, headers = headers}

    json_body, error = response.json()

    quote_name = json_body.hits.hits[1]._source.symbol
    --quote_name = ""

    base_query=queries.asset_query(v.base)
    response = requests.get{'https://elasticsearch.bitshares-kibana.info/_search', data=base_query, headers = headers}

    json_body, error = response.json()

    -- DeepPrint(json_body.hits.hits[2])

    base_name = json_body.hits.hits[1]._source.symbol


    response = requests.get{'http://185.208.208.184:5000/get_ticker?base=BTS&quote=' .. quote_name .. '', headers = headers}
    json_body, error = response.json()

    print(quote_name .. "/" .. base_name .. "," .. v.quote_amount/v.base_amount .. "," .. json_body.latest .. "," .. (v.quote_amount/v.base_amount)*json_body.latest)



    -- print(base_name)
    -- print(quote_name)





end
