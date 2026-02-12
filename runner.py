import json

def load_schedule(path):
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events

def handle_2pl(events, initial_state):
    transactions = set()
    items = {}
    for i in initial_state.keys():
        items[i] = []

    step = 1
    trace = []
    wait_queues = {k: [] for k in items}
    txn_locks = {}

    def _append(event):
        nonlocal step
        trace.append(event)
        step += 1

    def can_grant(t, item, mode):
        holders = items[item]
        if not holders:
            return True, None

        # if t already holds a lock
        for (owner, m) in holders:
            if owner == t:
                # already has same mode
                if m == mode:
                    return True, None
                # upgrade S -> X: only possible if no other holders
                if m == "S" and mode == "X":
                    # check if any other owner exists
                    for (o2, _) in holders:
                        if o2 != t:
                            return False, f"waiting for X({item})"
                    return True, None
                return True, None

        # t does not hold anything
        if mode == "S":
            # shared can be granted if no X holder
            for (owner, m) in holders:
                if m == "X" and owner != t:
                    return False, f"waiting for X({item})"
            return True, None
        else:  # X
            # exclusive requires no other holders
            for (owner, _) in holders:
                if owner != t:
                    return False, f"waiting for X({item})"
            return True, None

    def grant_lock(t, item, mode):
        # add to items and txn_locks and emit LOCK
        lock_evt = {"step": step, "event": "LOCK", "item": item, "grant": mode, "to": t}
        _append(lock_evt)
        # replace existing S->X for t if present
        replaced = False
        for idx, (owner, m) in enumerate(items[item]):
            if owner == t:
                items[item][idx] = (t, mode)
                replaced = True
                break
        if not replaced:
            items[item].append((t, mode))

        txn_locks.setdefault(t, set()).add((item, mode))

    def release_locks(t):
        # release all locks held by t and try to wake waiters
        held = list(txn_locks.get(t, []))
        for (item, mode) in held:
            # remove from items[item]
            items[item] = [(o, m) for (o, m) in items[item] if o != t]
            unlock_evt = {"step": step, "event": "UNLOCK", "item": item, "t": t, "mode": mode}
            
            _append(unlock_evt)
            idx = 0
            # continue scanning queue and try to grant those that can now be granted
            while idx < len(wait_queues[item]):
                req = wait_queues[item][idx]
                can, reason = can_grant(req['t'], item, req['mode'])
                if can:
                    # grant and remove from queue
                    grant_lock(req['t'], item, req['mode'])
                    unblock_evt = {"step": step, "event": "UNBLOCK", "t": req['t'], "op": req['evt']['op'], "item": item}
                    
                    _append(unblock_evt)
                    
                    # perform the blocked operation now
                    if req['evt']['op'] == 'R':
                        op_evt = {"step": step, "event": "OP", "t": req['t'], "op": "R", "item": item, "value": initial_state.get(item)}
                        
                        _append(op_evt)
                    elif req['evt']['op'] == 'W':
                        val = req['evt'].get('value')
                        initial_state[item] = val
                        op_evt = {"step": step, "event": "OP", "t": req['t'], "op": "W", "item": item, "value": val}
                        
                        _append(op_evt)
                    wait_queues[item].pop(idx)
                else:
                    idx += 1
        # clear txn_locks entry
        txn_locks.pop(t, None)

    for evt in events:
        curr_trace = {"step": step}
        operation = evt['op']

        if operation == "BEGIN":
            t_num = int(evt['t'])
            transactions.add(t_num)
            curr_trace.update({"event": "OP", "t": t_num, "op": "BEGIN"})
            _append(curr_trace)

        elif operation == "R":
            t_num = int(evt['t'])
            item = evt.get('item')
            can, reason = can_grant(t_num, item, "S")
            if can:
                # grant (may be no-op if already held)
                grant_lock(t_num, item, "S")
                op_evt = {"step": step, "event": "OP", "t": t_num, "op": "R", "item": item, "value": initial_state.get(item)}
                _append(op_evt)
            else:
                # blocked: record op with BLOCKED result and queue the request
                op_evt = {"step": step, "event": "OP", "t": t_num, "op": "R", "item": item, "result": "BLOCKED", "why": reason}
                _append(op_evt)
                wait_queues[item].append({'t': t_num, 'mode': 'S', 'evt': evt})

        elif operation == "W":
            t_num = int(evt['t'])
            item = evt.get('item')
            val = evt.get('value')
            can, reason = can_grant(t_num, item, "X")
            if can:
                grant_lock(t_num, item, "X")
                # perform write
                initial_state[item] = val
                op_evt = {"step": step, "event": "OP", "t": t_num, "op": "W", "item": item, "value": val}
                _append(op_evt)
            else:
                op_evt = {"step": step, "event": "OP", "t": t_num, "op": "W", "item": item, "value": val, "result": "BLOCKED", "why": reason}
                _append(op_evt)
                wait_queues[item].append({'t': t_num, 'mode': 'X', 'evt': evt})

        elif operation == "COMMIT":
            t_num = int(evt['t'])
            curr_trace.update({"event": "OP", "t": t_num, "op": "COMMIT"})
            _append(curr_trace)
            release_locks(t_num)

    return trace, initial_state

def handle_mvcc(events, initial_state):
    versions = {k: [(v, 0)] for k, v in initial_state.items()}
    txns = {} 

    step = 1
    trace = []

    def _append(evt):
        nonlocal step
        trace.append(evt)
        step += 1

    for evt in events:
        op = evt['op']

        if op == 'BEGIN':
            t = int(evt['t'])
            txns[t] = {'start': step, 'writes': {}, 'state': 'active'}
            _append({'step': step, 'event': 'OP', 't': t, 'op': 'BEGIN'})

        elif op == 'R':
            t = int(evt['t'])
            item = evt.get('item')
            txn = txns.get(t)
            if not txn or txn['state'] != 'active':
                _append({'step': step, 'event': 'OP', 't': t, 'op': 'R', 'item': item, 'value': None})
                continue

            start_ts = txn['start']
            # find latest committed version visible to this txn
            val = None
            for (v, cts) in reversed(versions[item]):
                if cts <= start_ts:
                    val = v
                    break
            # if txn has a pending write for this item, prefer that (read-your-writes)
            if item in txn['writes']:
                val = txn['writes'][item]

            _append({'step': step, 'event': 'OP', 't': t, 'op': 'R', 'item': item, 'value': val})

        elif op == 'W':
            t = int(evt['t'])
            item = evt.get('item')
            val = evt.get('value')
            txn = txns.setdefault(t, {'start': step, 'writes': {}, 'state': 'active'})
            if txn['state'] != 'active':
                _append({'step': step, 'event': 'OP', 't': t, 'op': 'W', 'item': item, 'value': val, 'result': 'ABORT'})
                continue

            # buffer the write (no visible effect until commit)
            txn['writes'][item] = val
            _append({'step': step, 'event': 'OP', 't': t, 'op': 'W', 'item': item, 'value': val})

        elif op == 'COMMIT':
            t = int(evt['t'])
            txn = txns.get(t)
            if not txn or txn['state'] != 'active':
                _append({'step': step, 'event': 'OP', 't': t, 'op': 'COMMIT', 'result': 'ABORT', 'why': 'not active'})
                continue

            # check write-write conflict
            conflict = False
            for item in txn['writes']:
                latest_cts = versions[item][-1][1]
                if latest_cts > txn['start']:
                    conflict = True
                    break

            if conflict:
                _append({'step': step, 'event': 'ABORT', 't': t})
                _append({'step': step, 'event': 'OP', 't': t, 'op': 'COMMIT', 'result': 'ABORT', 'why': 'conflict'})
                txn['state'] = 'aborted'
            else:
                commit_ts = step
                # install writes
                for item, val in txn['writes'].items():
                    versions[item].append((val, commit_ts))

                _append({'step': step, 'event': 'OP', 't': t, 'op': 'COMMIT'})
                txn['state'] = 'committed'

    final_state = {item: versions[item][-1][0] for item in versions}
    return trace, final_state

def run_schedule(schedule_path, cc, initial_state):
    events = load_schedule(schedule_path)
    # dispatch to the chosen concurrency control
    if cc == "2pl":
        trace, final_state = handle_2pl(events, initial_state)
    else:
        trace, final_state = handle_mvcc(events, initial_state)

    return trace, final_state