local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local item_patterns = {
  ["^https?://[^/]*myktoon%.com/web/league/detail_list%.kt%?lguewebtoonseq=([0-9]+)"]="series",
  ["^https?://[^/]*myktoon%.com/web/league/viewer%.kt%?lguetimesseq=([0-9]+)"]="episode",
  ["^https://([^/]*myktoon%.com/css[0-9]*/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/js[0-9]*/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/images[0-9]*/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/common[0-9]*/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/web/css/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/web/js/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/web/images/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/web/common/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/moweb/css/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/moweb/js/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/moweb/images/.+)$"]="asset",
  ["^https://([^/]*myktoon%.com/moweb/common/.+)$"]="asset",
  ["^https://(cds%.myktoon%.com/.+)$"]="asset"
}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs(item_patterns) do
    if not string.match(pattern, "%$$") then
      pattern = pattern .. "$"
    end
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "^https?://v2%.myktoon%.com/.") then
    return false
  end

  local skip = false
  for pattern, type_ in pairs(item_patterns) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  for _, pattern in pairs({
    "([0-9]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  local post_data = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_ .. tostring(post_data))
      and allowed(url_, origurl) then
      local headers = {}
      if post_data then
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        headers["Accept"] = "application/json, text/javascript, */*; q=0.01"
      elseif string.match(url_, "ajax%.kt") then
        return nil
      end
      if post_data then
        table.insert(urls, {
          url=url_,
          headers=headers,
          body_data=post_data,
          method="POST"
        })
      else
        table.insert(urls, {
          url=url_,
          headers=headers
        })
      end
      addedtolist[url_ .. tostring(post_data)] = true
      addedtolist[url .. tostring(post_data)] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  local function queue_post(newurl, params)
    local params_s = ""
    for k, v in pairs(params) do
      if string.len(params_s) > 0 then
        params_s = params_s .. "&"
      end
      if type(v) ~= "number" and type(v) ~= "string" then   
        error("Bad value type.")
      end
      params_s = params_s .. k .. "=" .. tostring(v)
    end
    post_data = params_s
    ids[newurl] = true
    check(newurl)
    check(newurl .. "?" .. params_s)
    post_data = nil
  end

  local function queue_special_post(start_no)
    if item_type == "series" then
      queue_post(
        "https://www.myktoon.com/web/league/leadgue_detail_list_ajax.kt",
        {
          ["lguewebtoonseq"]=item_value,
          ["startCnt"]=tostring(start_no),
          ["turmCnt"]=context["turm_no"],
          ["sortfg"]="up"
        }
      )
    elseif item_type == "episode" then
      return queue_post(
        "https://www.myktoon.com/mw/league_reply/reply_list_ajax.kt",
        {
          ["targetseq"]=item_value,
          ["sort"]="good",
          ["startNo"]=tostring(start_no),
          ["turmNo"]=context["turm_no"]
        }
      )
    end
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset" then
    html = read_file(file)
    if string.match(url, "_ajax%.kt") then
      json = cjson.decode(html)
      if json["code"] ~= "SUC_PROC_0000" then
        error("Unsuccesfully received data.")
      end
      if string.match(url, "/leadgue_detail_list_ajax%.kt")
        or string.match(url, "/reply_list_ajax%.kt") then
        local count = get_count(json["response"])
        if string.match(url, "%?")
          and string.match(url, "[%?&]start[A-Za-z]+=[0-9]")
          and count > 0 then
          queue_special_post(
            tonumber(
              string.match(url, "startCnt=([0-9]+)")
              or string.match(url, "startNo=([0-9]+)")
            ) + count
          )
        end
        if item_type == "series" then
          for _, v in pairs(json["response"]) do
            check("https://www.myktoon.com/web/league/viewer.kt?lguetimesseq=" .. tostring(v["lguetimesseq"]))
          end
        end
      end
    end
    if not context["turm_no"]
      and (
        string.match(url, "/detail_list%.kt%?lguewebtoonseq=")
        or string.match(url, "/viewer%.kt%?lguetimesseq=")
      ) then
      local turm_no = string.match(html, "leguObj%.turmNo%s*=%s*([0-9]+)")
        or string.match(html, "replyObj%.turnCnt%s*=%s*([0-9]+)")
      if not turm_no then
        error("Could not get turmNo.")
      end
      context["turm_no"] = turm_no
    end
    if item_type == "series" then
      if context["turm_no"] then
        queue_special_post(0)
      end
      queue_post(
        "https://www.myktoon.com/web/league/leadgue_detail_list_ajax.kt",
        {
          ["lguewebtoonseq"]=item_value,
          ["sortfg"]="first"
        }
      )
    end
    if item_type == "episode" then
      if context["turm_no"] then
        queue_special_post(0)
      end
      if string.match(url, "/viewer%.kt%?lguetimesseq=") then
        queue_post(
          "https://www.myktoon.com/web/league/ltimes_image_list_ajax.kt",
          cjson.decode(string.match(html, "url%s*:%s*'/web/league/ltimes_image_list_ajax%.kt',%s*dataType%s*:%s*'json',%s*data%s*:%s*({.-}),"))
        )
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] == 400 then
    error("Aborting on 400 immediately.")
  end
  if http_stat["statcode"] ~= 200 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 6
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["ktoon-mc2yeuge259fxpul"] = discovered_items,
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


