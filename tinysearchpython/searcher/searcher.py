from flask import Flask, render_template, request


app = Flask(__name__)

@app.route("/")
def search():
    thequery = request.args.get("query", None)
    if not thequery:
        return render_template('search.html', query=thequery, results=None)

    
    return render_template('search.html', results=[1, 2, 3, 4, 5])

app.run(host='0.0.0.0', port=8000)