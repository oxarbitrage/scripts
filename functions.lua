local F = {}

local function loop_fills(hits)

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
                    markets[k2].trades = markets[k2].trades + 1
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
                base_amount = v._source.operation_history.op_object.receives.amount,
                trades = 1
            }
        end
    end

end

-- Utility function to print a recursive table keys and values
local function DeepPrint (e)
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

F.loop_fills = loop_fills
F.DeepPrint = DeepPrint

return F