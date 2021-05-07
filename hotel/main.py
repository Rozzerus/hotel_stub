import flask

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/status', methods=['GET'])
def home():
    return "<h1>Hotel Stub A</h1><p>This hotel stub is working</p>"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
