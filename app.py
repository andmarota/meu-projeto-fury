from flask import Flask

app = Flask(__name__)

@app.route('/ping')
def ping():
    return 'pong', 200

@app.route('/')
def home():
    return 'Meu servico Fury esta funcionando!', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
