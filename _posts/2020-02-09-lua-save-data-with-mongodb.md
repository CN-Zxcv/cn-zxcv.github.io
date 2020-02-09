---

title: lua 中使用 mongodb 保存数据
tag: lua mongdb
key: review-2020-02-09-lua-save-data-with-mongodb

---

在使用 c + lua + mongodb 开发游戏服务器的时候，在上面实现了一套数据库读写方案，做到每帧落地，使用起来感觉还行。

<!--more-->

首先是从数据库读数据，这个就比较简单了，lua的数据结构和mongodb基本就是一致的，数据读取出来后就是一个table，基本上直接设置上metatable，包装成一类就可以使用。

这里需要注意的一个地方就是mongodb中的Array：如果luatable使用number做key，在保存的时候会被转换为Array，如果key值不连续还比较大，在保存的时候就会生成一个极大的Array，所以在保存的时候将key转换为string是比较好的做法，读取的时候再转换回来，我使用的bson库是云风的<https://github.com/cloudwu/lua-bson>  修改了一点，实现了上述的转换，不在lua层做这个事情，lua的代码也可以简洁不少。



然后是如何保存数据，我采用的是记录数据的变化（增加/删除/修改），然后在帧尾统一保存。

因为使用的是mongodb，数据在保存的时候并不需要进行转换，直接通过upsert的方式就可以写入，通过upsert就覆盖了增加/修改两个场景，如何记录删除呢，特别是在一帧中对数据进行多次操作的时候？

例如有数据 a = {b = {c = 1}}，

先在b下添加 a = {b = {c1 = 1}}，语句是 set "a.b.c1" = 1

然后删除b：a = {}，语句是 unset “a.b" = 1

再添加b：a = {b = {c2 = 1}}，语句是  set "a.b" = {c2=1}，这里注意上一层unset过的话，再设置数据上去就不应该只处理子节点了。

从上面可以看出，增删是有层级关系的，上层的删操作会覆盖下层，所以需要一个数据对应的结构来记录每层进行过的操作。考虑到需要保存的数据理论上只是数据，不应该存在meta，这里为了简单点，就直接在数据上使用meta来打标记。

记录变化的主要代码如下

```lua
-- 变化的数据
__save_data = {}

-- 标记数据变化的meta
__mt_pending = __mt_pending or {}
__mt_pending.__index = function(self, k)
    local t = setmetatable({}, __mt_pending)
    self[k] = t
    return t
end

__mt_delete = __mt_delete or {}
__mt_delete.__index = function(self, k)
    local t = setmetatable({}, __mt_pending)
    self[k] = t
    return t
end

-- 如果需要的话，不同的handle映射不同的数据库，比如分为本服数据库，全局数据库
-- Save = new_handler('local')
-- 添加
-- Save.Add('player', 1, pid, 100)
-- 删除
-- Save.Del('player', 1, pid)
-- Add相对用的更多，通过__index实现一个更简单的用法
-- Save.player[1].pid = 100
function new_handler(tip)
    local handle = {
        __tip = tip,
        __pending = setmetatable({}, __mt_pending), -- 当前帧改动数据
        __raw_cmds = {},                            -- 当前帧直接的指令(优先于pending执行)
        __frame_data = {},                          -- 还未入库的改动
        __frame_queue = {},                         -- 顺序的frame_data
        __ack_window = {},                          -- 流控窗口(基于顺序写入是顺序返回的)
        Add = pending_add,                          -- 添加
        Del = pending_del,                          -- 删除
    }

    setmetatable(handle, {
        __index = function(t, k)
            return handle.__pending[k] 
        end
    })

    _save_data[tip] = handle
    return handle
end

-- 通过 __index触发函数调用，在__pending上自动生成记录层级，并为数据挂上meta，
-- 有meta的数据即表示有改动，通过所有的meta描述了一个改动的树结构
function _pending_add(t, k, v, tail, ...)
    if tail == nil then
        t[k] = v
    else
        if not t[k] then 
            t[k] = setmetatable({}, __mt_pending)
        end
        _pending_add(t[k], v, tail, ...)
    end
end

function pending_add(data, ...)
    local args = {...}
    assert(#args == table.size(args), string.fmt('%q', args))
    _pending_add(data.__pending, ...)
end

-- 标记对应的节点被删除
function _pending_del(t, k, tail, ...)
    if not t[k] then 
        t[k] = setmetatable({}, __mt_pending)
    end

    if not getmetatable(t) then
        setmetatable(t, __mt_pending)
    end

    if not tail then 
        t[k] = setmetatable({}, __mt_delete)
        return 
    end

    _pending_del(t[k], tail, ...)
end

function pending_del(data, ...)
    local args = {...}
    assert(#args == table.size(args))
    _pending_del(data.__pending, ...)
end
```

有了上述代码，我们就可以标记数据的变化，然后在帧尾将变化转换为数据库操作，其实就是通过打包为一个大的bson，一次性发送多条cmd给数据库。

```lua
WRITE_CONCERN = {w=1,wtimeout=5000}

_save_data = {}

function save()
   for k, v in pairs(_save_data) do
       save_data(v)
   end
end

function save_data(data, frame)
    if table.is_empty(data.__raw_cmds) and table.is_empty(data.__pending) then
        -- nothing to do
        return
    end

    local frame_data = data.__frame_data[frame]
    if not frame_data then
        frame_data = {
            __frame = frame,   -- 帧序号
            __cmds = {},       -- pending进来的
            __raws = {},       -- 直接加的
            __ncmd = 0,        -- 总数
            __nsend = 0,       -- 发出去的请求数量
            __nrecv = 0,       -- 收到的ack数量
        }
        data.__frame_data[frame_data.__frame] = frame_data
        table.insert(data.__frame_queue, frame_data)
    end

    -- raw_cmds记录进来
    table.insert(frame_data.__raws, data.__raw_cmds)
    frame_data.__ncmd = frame_data.__ncmd + #data.__raw_cmds
    data.__raw_cmds = {}

    -- 遍历重置__pending，生成数据库操作
    local pending = data.__pending
    data.__pending = setmetatable({}, __mt_pending)

    for collname, coll in pairs(pending) do
        local upds = {}
        local dels = {}

        for recid, rec in pairs(coll) do
            local __mt = getmetatable(rec)
            if __mt == __mt_delete then
                if table.is_empty(dels) then
                    table.insert(dels, {
                        q = {_id = recid},
                        limit = 1,
                    })
                else
                    table.insert(upds, {
                        q = {_id = recid},
                        u = {
                            ['$set'] = rec, 
                        },
                        upsert = true,
                        multi = false,
                    })
                end
            elseif __mt == __mt_pending then
                local chgs = {set={}, unset={}}
                for k, v in pairs(rec) do
                    check_value_chgs(chgs, k, v)
                end

                table.insert(upds, {
                    q = {_id = recid},
                    u = {
                        ['$set'] = (not table.is_empty(chgs.set)) and chgs.set or nil,
                        ['$unset'] = (not table.is_empty(chgs.unset)) and chgs.unset or nil,
                    },
                    upsert = true,
                    multi = false,
                })
            else
                table.insert(upds, {
                    q = {_id = recid},
                    u = {
                        ['$set'] = rec, 
                    },
                    upsert = true,
                    multi = false,
                })
            end

            -- debug.dump(upds, 'dbmng.upds')
            -- debug.dump(dels, 'dbmng.dels')

            -- 每1024个改动生成一个语句，所有改动生成一个超大的cmd并不正确，另外bson也是有长度限制的，
            if #upds >= 1024 then
                build_bson_cmd_update(frame_data.__cmds, collname, upds)
                upds = {}
            end
            if #dels >= 1024 then
                build_bson_cmd_delete(frame_data.__cmds, collname, dels)
                dels = {}
            end
        end

        build_bson_cmd_update(frame_data.__cmds, collname, upds)
        build_bson_cmd_delete(frame_data.__cmds, collname, dels)
    end
    frame_data.__ncmd = frame_data.__ncmd + #frame_data.__cmds

    -- loop_write(data)
end

function check_value_chgs(chgs, key, val)
    if type(val) == 'table' then
        local __mt = getmetatable(val)
        if __mt == __mt_pending then
            for k, v in pairs(val) do
                check_value_chgs(chgs, string.format('%s.%s',key, k), v)
            end
        elseif __mt == __mt_delete then
            if table.is_empty(val) then
                chgs.unset[key] = 1
            else
                for k, v in pairs(val) do
                    check_value_chgs(chgs, string.format('%s.%s',key, k), v)
                end
            end
        else
            chgs.set[key] = val
        end
        
    else
        chgs.set[key] = val
    end
end

function build_bson_cmd_update(cmds, collname, docs)
    if table.is_empty(docs) then 
        return 
    end

    local ret, cmd = xpcall(bson.encode_order, __G__TRACEBACK__, 'update', collname, 'updates', docs, 'ordered', false, 'writeConcern', WRITE_CONCERN)
    if not ret then
        -- 生成失败的话拆分成二分后再次尝试生成，尽量排除掉生成失败的代码
        local len = #docs
        if len > 1 then
            build_bson_cmd_update(cmds, collname, tool.take(docs, 1, math.floor(len/2)))
            build_bson_cmd_update(cmds, collname, tool.take(docs, math.ceil(len/2), len))
        else
	        -- ERROR('build_bson_cmd_update, failed')
            -- debug.dump(cmd)
        end
    else
        table.insert(cmds, cmd)
    end
end

function build_bson_cmd_delete(cmds, collname, docs)
    if table.is_empty(docs) then
        return
    end
    
    local ret, cmd = xpcall(bson.encode_order, __G__TRACEBACK__, 'delete', collname, 'deletes', docs, 'ordered', false, 'writeConcern', WRITE_CONCERN)
    if not ret then
        local len = #docs
        if len > 1 then
            build_bson_cmd_delete(cmds, collname, tool.take(docs, 1, math.floor(len/2)))
            build_bson_cmd_delete(cmds, collname, tool.take(docs, math.ceil(len/2), len))
        else
	        -- ERROR('build_bson_cmd_delete, failed')
            -- debug.dump(cmd)
        end
    else
        table.insert(cmds, cmd)
    end
end


```

最后就是保存数据，发送消息，等待回复。当然需要实现一定的流控和重试策略，在数据库异常情况下不至于丢失数据。

这里并没有去考虑说数据库不在了就写到文件之类的方式，在我看来意义不大。这里的策略是只要数据库最终能连上来，就不会丢数据。

```lua
-- 持有的数据库连接
_dbconn_map = _dbconn_map or {}
-- 流控，允许同时进行多少个写入帧，超过后不再继续发送写操作
ACK_WINDOW = 2

SAVE_STATE = {
    NONE = 0,
    SENT = 1,
}

function conn_new(conn, collection)
    local t = {
        tip = conn.tip,
        conn = conn,
        db = collection,
    }
    if not _dbconn_map[t.tip] then
        _dbconn_map[t.tip] = {}
    end
    table.insert(_dbconn_map[t.tip], t)
    gCoPool:resume_lru('wait_mongo')
end

function rand_idx(total, policy)
    if total == 0 then 
        return nil 
    end
        
    policy = policy or total
    return (policy % total) + 1
end

function get_db(tip, policy)
    local conns = _dbconn_map[tip] or {}

    if #conns == 0 then
        WARN('dbmng.get_db, not ready, tip=%s', tip)
        gCoPool:yield_lru('wait_mongo')
    end

    local conn = conns[rand_idx(#conns, policy)]
    return conn.db
end

-- 这里用了一些协程处理，作用是在等待数据库回复的时候让出，执行其他逻辑，
-- loop_write也是在协程中执行的
function loop_write(data)
    while true do
        local frame_data = data.__frame_queue[1]
        if not frame_data then
            break
        end
        if #data.__ack_window > ACK_WINDOW then
            local sz = #data.__frame_queue
            local msg = string.format('dbmng.loop_write, limit by ACK_WINDOW, nwait=%s', #data.__frame_queue)
            if sz > ACK_WINDOW * 50 and sz % 50 == 0 then
                ERROR(msg)
            elseif sz > ACK_WINDOW * 5 and sz % 5 == 0 then
                WARN(msg)
            else
                INFO(msg)
            end
            break
        end

        table.insert(data.__ack_window, table.remove(data.__frame_queue, 1))
        
        local db = dbmng.get_db(data.__tip)
        for _, v in ipairs(frame_data.__raws) do
            for _, cmd in ipairs(v) do
                gCoPool:pull(write_thread, data, frame_data, db, cmd)
            end
        end

        for _, cmd in pairs(frame_data.__cmds) do
            gCoPool:pull(write_thread, data, frame_data, db, cmd)
        end
        frame_data.__state = SAVE_STATE.SENT
    end
end

function write_thread(data, frame_data, db, bson_cmd)
    frame_data.__nsend = frame_data.__nsend + 1
    local info = db:runCommandBson(bson_cmd)
    frame_data.__nrecv = frame_data.__nrecv + 1

    local ack_window = data.__ack_window[1]
    if ack_window ~= frame_data then
        CRIT('dbmng.write_thread, ack_window, missing??, sendframe=%s, gFrame=%s, ncmd=%s, nsend=%s, nrecv=%s'
        , frame_data.__frame, gFrame, frame_data.__ncmd, frame_data.__nsend, frame_data.__nrecv)
    elseif frame_data.__nrecv >= frame_data.__nsend then
        DEBUG('dbmng.save, finish, frame=%s, gFrame=%s', frame_data.__frame, gFrame)
        table.remove(data.__ack_window, 1)
    end

    --TODO: redo when return specific fail code
    debug.dump(info)
    if info.ok ~= 1 then
        ERROR('dbmng.save, failed, ok=%s, code=%s, errmsg=%s', info.ok, info.code, info.errmsg)
    else
        
    end
end
```

有了这套落库方式，处理数据库读写简单了不少，更进一步，考虑我们要保存数据其实就是要检测到数据的变动，luatable和mongodb又挺一致的，如果我们能实现检测luatable的属性变动，自动调用Save，在写逻辑时就方便了不少，这个通过meta很容易实现，我这里采用的方式是实现一个可以检查变动的对象，所有需要保存的数据实现包装为此对象，就可以自动保存。

```lua
-- class实现了类，单继承结构，mark是用于标记同类的对象创建了多少个的，通过在lua c实现中注入代码来实现，这里忽略并不影响主题
-- 包含了两个创建方法，deliver/new和wrap，
-- deliver/new用于直接创建，
-- wrap是把数据包装成类，主要就是用于从数据库读取的数据中恢复类对象
-- 拥有两个静态属性_base和_example
-- _example用于标记哪些数据需要检测变动，这些数据在类中被保存到_pro上，通过meta触发检查
-- _base指向基类

local function _trans(d, r, example)
    for k, v in pairs(r) do
        if example and example[k] ~= nil then
            d._pro[k] = v
        else
            rawset(d, k, v)
        end
    end
end
-- 我使用的是lua5.3，并且开启了module兼容，既可以使用module也可以使用env
-- 使用module的原因是module在热更新的时候很好用，也比较习惯了
-- 使用lua5.3的原因是lua5.1 attempt to yield across metamethod/C-call boundary，我大量使用了协程，很难受，升级是必须的
function class(env, name, mark)
    if name then
        local base = table.find(_G, table.unpack(string.split(name, '%.')))
        assert(base, string.format("base class not found, %s", name))
        env._base = setmetatable({}, {
            __index=function(t, k) 
                if base[k] then return base[k] end
                if base._base then return base._base[k] end
            end,
        })
    end

    for k, v in pairs(env._base and env._base._example or {}) do
        if not env._example then env._example = {} end
        env._example[k] = env._base._example[k]
    end

    env.init = function(self) 
        if env._base then env._base.init(self) end
    end

    env._meta = {
        __index=function(t, k)
            if not rawget(t, '_pro') then
                error('_pro is missing')
            elseif t._pro[k] ~= nil then
                return t._pro[k]
            else
                local v = rawget(t, k) or rawget(env, k)
                if v then return v end
                return env._base and env._base[k]
            end
        end,
        __newindex=function(t, k, v)
            if env._example and env._example[k] ~= nil then
                if (not t._init) and t.on_value_change then
                    t:on_value_change(k, t._pro[k], v)
                end
                t._pro[k] = v
                return
            end
            rawset(t, k, v)
        end
    }

    env.new = function(...)
        local o = mark and mark() or {}
        o._pro = table.copy(env._example) or {}
        setmetatable(o, env._meta)
        o._init = true
        o:init()
        if o.ctor then o:ctor(...) end
        o._init = nil
        return o
    end
    
    env.wrap = function(t)
        local o = mark and mark() or {}
        o._pro = table.copy(env._example) or {}
        setmetatable(o, env._meta)
        o._init = true
        o.init(o)
        local pro = rawget(t, '_pro')
        if pro then
            _trans(o, pro, env._example)
            rawset(t, "_pro", nil)
        end
        _trans(o, t, env._example)
        o._init = nil
        return o
    end
end
```

使用方式如下

```lua
module("player", package.seeall)
_example = {
    _id = 0,
    pid = 0,
    name = '',
    data = {},
}
class(_ENV, nil, nil)

function init(self)
	self.temp = 1
end

function on_value_change(self, key, old, new)
    Save.player[self._id][key] = new
end

--
local A = player.new()
-- 修改name会触发meta，调用on_value_change，最终落库
A.name = 'hello'
-- 这个并不会触发meta
A.temp = 2

-- 这样并不会触发
A.data.key = 'value'
-- 我们可以这样强制触发
A.data = A.data
-- 或者手动调用Save
Save.player[A._id].data.key = A.data.key
```

到这里，实现了一个比较好用的落库做法，可以看到还是又些不完美的地方，比如在处理子层级的时候我们还是需要手动调用save，但是解决了80%的问题。游戏服务器里面数据并不是很复杂，层级不多，层级内的数据也不太多，通常直接A.data = A.data简单粗暴。

因为class的实现多了一个间层，然后每帧都会去写数据库，性能可能跟不上，但是实测没啥影响，性能热点不在这里，每帧的数据变动也没有想象那么多，反而因为落地即时，即使服务器挂掉也只会丢失几帧数据，这点上来说还是很重要。

不知道有没有其他更好的方案？
