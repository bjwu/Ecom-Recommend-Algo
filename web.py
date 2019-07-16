from flask import Flask, render_template, request, url_for, redirect
from kafka import KafkaProducer
import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

k = KafkaProducer(bootstrap_servers='localhost:9092')

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        user = request.form['user']
        return redirect(url_for('show_user_rec', username=user))
    return render_template("index.html")

@app.route('/user/<username>', methods=['GET', 'POST'])
def show_user_rec(username):
    # TODO: latest要改成rec
    items = [int(i[:-2]) for i in r.lrange("latest:"+ str(int(username)), 0, 2)]
    if request.method == 'POST':
        newitem = request.form['item']
        newlog = ','.join([str(username),str(newitem)])
        k.send('logFromWeb', newlog.encode('utf-8'))
        time.sleep(1)
        items = [int(i[:-2]) for i in r.lrange("latest:"+ str(int(username)), 0, 2)]
    return render_template("rec.html", user=username, item0=items[0], item1=items[1], item2=items[2])


if __name__ == '__main__':
    app.debug = True
    app.run()
