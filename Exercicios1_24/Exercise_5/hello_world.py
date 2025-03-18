from flask import Flask
app = Flask(__name__) 

@app.route("/")
def helloworld():
    return "Hello World!"

if __name__ == "__main__":
    app.run(port=5000, debug=True)

#1º Tenho de correr o código python 
#2º Agora no browser digite http://127.0.0.1:5000