import flask
import csv
import threading
import socket
import time
from datetime import datetime

host = '0.0.0.0'
port = 8080

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/status', methods=['GET'])
def home():
    return "<h1>Hotel Stub C</h1><p>This hotel stub is working</p>"


def start_web(name):
    print(f"Thread {name}: starting")
    socket_host = host
    socket_port = port+1000
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('socket_host and port are ', socket_host, socket_port)
    s.bind((socket_host, socket_port))
    s.listen(1)
    print('\nListening for a client at', host, socket_port)
    conn, addr = s.accept()
    while True:
        print('\nConnected by', addr)
        try:
            print('\nReading file...\n')
            with open('hotel_price_c.csv') as file:
                reader = csv.reader(file)
                next(reader, None)
                for line in reader:
                    out = str(line[0]) + ',' + str(line[1]) + ',' + str(line[2]) + ';'
                    print('Sending line', out)
                    out = str(out).encode()
                    conn.sendall(out)
                    time.sleep(1)
                file.close()
            time.sleep(10)
            print('End Of Stream.')
            conn.close()
        except socket.error:
            print('Error Occured.\n\nClient disconnected.\n')
    print(f"Thread {name}: finishing")


if __name__ == '__main__':
    threading.Thread(target=start_web, args=('socket',), daemon=True).start()
    app.run(host=host, port=port)

