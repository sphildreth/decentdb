local M = {}

function M.slugify(value)
  value = string.lower(value)
  value = string.gsub(value, "[^a-z0-9]+", "-")
  value = string.gsub(value, "^-+", "")
  value = string.gsub(value, "-+$", "")
  return value
end

function M.split_words(value)
  local rows = {}
  for word in string.gmatch(value or "", "%S+") do
    table.insert(rows, { word = word })
  end
  return rows
end

function M.lua_sum_step(state, value)
  return (state or 0) + (value or 0)
end

function M.lua_sum_final(state)
  return state or 0
end

function M.reverse_text(left, right)
  if left == right then return 0 end
  if left > right then return -1 end
  return 1
end

return M
