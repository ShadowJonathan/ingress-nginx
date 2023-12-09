local util = require("util")
local split = require("util.split")
require("resty.core")

local ngx = ngx
local ipairs = ipairs
local setmetatable = setmetatable
local string_format = string.format
local ngx_log = ngx.log
local DEBUG = ngx.DEBUG

local TTL_1H = 60 * 60

local _M = { name = "leastconn" }

function _M.new(self, backend)
    local o = {
        peers = backend.endpoints,
        traffic_shaping_policy = backend.trafficShapingPolicy,
        alternative_backends = backend.alternativeBackends,
    }
    setmetatable(o, self)
    self.__index = self
    return o
end

function _M.is_affinitized()
    return false
end


local function get_upstream_name(upstream)
    return upstream.address .. ":" .. upstream.port
end


function _M.balance(self)
    local peers = self.peers
    local endpoint = peers[1]
    local endpoints = ngx.shared.balancer_leastconn
    local feasible_endpoints = {}

    if #peers ~= 1 then
        local lowestconns = 2147483647
        -- find the lowest connection count
        for _, peer in pairs(peers) do
            local peer_name = get_upstream_name(peer)
            local peer_conn_count = endpoints:get(peer_name)
            if peer_conn_count == nil then
                -- Peer has never been recorded as having connections -
                -- add it to the list of feasible peers
                lowestconns = 0
                feasible_endpoints[#feasible_endpoints+1] = peer
            elseif peer_conn_count < lowestconns then
                -- Peer has fewer connections than any other peer evaluated so far - add it as the
                -- only feasible endpoint for now
                feasible_endpoints = {peer}
                lowestconns = peer_conn_count
            elseif peer_conn_count == lowestconns then
                -- Peer has equal fewest connections as other peers - add it to the list of
                -- feasible peers
                feasible_endpoints[#feasible_endpoints+1] = peer
            end
        end
        ngx_log(DEBUG, "select from ", #feasible_endpoints, " feasible endpoints out of ", #peers)
        endpoint = feasible_endpoints[math.random(1,#feasible_endpoints)]
    end

    local selected_endpoint = get_upstream_name(endpoint)
    ngx_log(DEBUG, "selected endpoint ", selected_endpoint)

    -- Update the endpoint connection count
    endpoints:incr(selected_endpoint, 1, 0, TTL_1H)
    endpoints:expire(selected_endpoint, TTL_1H)

    return selected_endpoint
end

function _M.after_balance(_)
    local endpoints = ngx.shared.balancer_leastconn

    -- if nginx has tried multiple servers, it has put them all through the balancer above
    -- so we need to tick off each server that has been tried, else they'll desync
    for _, upstream in pairs(split.split_upstream_var(ngx.var.upstream_addr)) do
        if not util.is_blank(upstream) then
            endpoints:incr(upstream, -1)
            endpoints:expire(upstream, TTL_1H)
        end
    end
end

function _M.sync(self, backend)
    self.traffic_shaping_policy = backend.trafficShapingPolicy
    self.alternative_backends = backend.alternativeBackends

    local normalized_endpoints_added, normalized_endpoints_removed =
        util.diff_endpoints(self.peers, backend.endpoints)

    if #normalized_endpoints_added == 0 and #normalized_endpoints_removed == 0 then
        return
    end

    ngx_log(DEBUG, string_format("[%s] peers have changed for backend %s", self.name, backend.name))

    self.peers = backend.endpoints
end

return _M
