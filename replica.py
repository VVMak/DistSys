import flask
import pickle
import sys

PORTS = set(14378, 14379, 14380, 14381)

assert sys.argv > 1
PORT = int(sys.argv[1])
assert PORT in PORTS
PORTS.discard(PORT)

MASTER = None

DUMP_FILE = f'r_{PORT}.csv'

def save_dict(d: dict):
    with open(DUMP_FILE, 'wb') as f:
        pickle.dump(d, f)

save_dict({})

def load_dict() -> dict:
    with open(DUMP_FILE, 'rb') as f:
        d = pickle.load(f)
    return d


app = flask.Flask(__name__)

@app.route('/get', methods=('GET'))
def get_request():
    return load_dict()

@app.route('/update', methods=('POST'))
def update_request():
    d = flask.request.json
    # if we are master, run broadcast
    # if there is no master, choose master
    # then try to send to master
    # if couldn't manage, choose master e.t.c.
    return 'Not implemented', 403

@app.route('/master_update', methods=('POST'))
def update_from_master():
    d = flask.request.json
    return 'Not implemented', 403

@app.route('/election', methods=('POST'))
def election():
    pass

app.run(host='0.0.0.0', port=PORT)
