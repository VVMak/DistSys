import itertools
import numpy as np
import random
import requests
import string

# ports
SERVER_PORTS = [14378, 14379, 14380, 14381, 14382]
NUM_OF_SERVERS = len(SERVER_PORTS)
MASTER_PORT = None

upd_port_gen = itertools.cycle(SERVER_PORTS)
get_port_gen = itertools.cycle(SERVER_PORTS)

KEY_LEN = 5
VALUE_LEN = 10
UPDATE_SIZE = 3
GET_SIZE = 3

REQ_TRIES = 2 * len(SERVER_PORTS)

def gen_strings(length, number):
    s = random.choices(string.ascii_letters, k=number*length)
    return list(map(''.join, np.split(np.array(s), number)))

def gen_upd_dict():
    keys = gen_strings(KEY_LEN, UPDATE_SIZE)
    values = gen_strings(VALUE_LEN, UPDATE_SIZE)
    return dict(zip(keys, values))

def gen_keys():
    return gen_strings(KEY_LEN, GET_SIZE)

def request_update(upd_dict, port) -> requests.Response:
    return requests.post(url=f'http://localhost:{port}/update', json=upd_dict)

def request_get(keys, port) -> requests.Response:
    return requests.get(url=f'http://localhost:{port}/get', json=keys)

def send_update():
    global MASTER_PORT
    data = gen_upd_dict()
    for _ in range(REQ_TRIES):
        if MASTER_PORT is None:
            MASTER_PORT = next(upd_port_gen)
        try:
            resp = request_update(data, MASTER_PORT)
            if resp.status_code == 200:
                return
            if resp.status_code >= 400:
                continue
            j = resp.json()
            if 'port' not in j:
                MASTER_PORT = None
                continue
            MASTER_PORT = j['port']
        except:
            MASTER_PORT = None
    raise RuntimeError('Server is not responding')

def send_get():
    keys = gen_keys()
    port = next(get_port_gen)
    request_get(keys, port)

def main():
    pass

main()
