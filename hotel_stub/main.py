import flask
import csv
import threading
import socket
from time import sleep

host = '0.0.0.0'
port = 8080

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/status', methods=['GET'])
def home():
    return "<h1>Hotel Stub A</h1><p>This hotel stub is working</p>"


def start_web(name):
    print(f"Thread {name}: starting")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_port = port+1000
    print('socket_port is ', socket_port)
    s.bind((host, socket_port))
    s.listen(1)
    while True:
        print('\nListening for a client at', host, socket_port)
        conn, addr = s.accept()
        print('\nConnected by', addr)
        try:
            print('\nReading file...\n')
            with open('hotel_price_a.csv') as file:
                reader = csv.reader(file)
                next(reader, None)
                for line in reader:
                    print('Sending line', line)
                    out = str(line[0]) + ' ' + str(line[1]) + ' ' + str(line[2]) + ' '
                    out = str(out).encode()
                    conn.send(out)
            sleep(10)
            print('End Of Stream.')
            conn.close()
        except socket.error:
            print('Error Occured.\n\nClient disconnected.\n')
    print(f"Thread {name}: finishing")


if __name__ == '__main__':
    threading.Thread(target=start_web, args=('socket',), daemon=True).start()
    app.run(host=host, port=port)

