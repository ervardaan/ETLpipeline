from flask import Flask
app=Flask(__name__)
@app.route("/trigger_etl")
def helloWorld():
    return("<p>Hello world</p>")