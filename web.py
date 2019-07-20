from flask import Flask, render_template, request, url_for, redirect
from kafka import KafkaProducer
import redis
import time
import pickle

r = redis.Redis(host='localhost', port=6379, db=0)

k = KafkaProducer(bootstrap_servers='localhost:9092')

f = open('./model/userset.pkl', 'rb')
userset_all = pickle.load(f)

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        user = request.form['user']
        if int(user) not in userset_all:
            return redirect(url_for('show_user_rec', username=user, new=True))
        return redirect(url_for('show_user_rec', username=user))
    return render_template("index.html")


@app.route('/user/<username>', methods=['GET', 'POST'])
def show_user_rec(username, new=False):
    msg = "Below are the items recommended for you"
    if new:
        items = [int(i) for i in r.zrange('hottest_one_week_before', 0, 2, desc=True)]
        msg = "Welcome! New Customer.\n These are the hottest items this week recommended for you"
        return render_template("rec.html", user=username, item0=items[0], item1=items[1], item2=items[2], message=msg)
    # TODO: latest要改成rec. items那个需要加try except
    items = [int(i[:-2]) for i in r.lrange("latest:"+ str(int(username)), 0, 2)]
    if request.method == 'POST':
        newitem = request.form['item']
        newlog = ','.join([str(username),str(newitem), 'test', str(88888), str(1)])
        k.send('logFromWeb', newlog.encode('utf-8'))
        time.sleep(1)
        items = [int(i[:-2]) for i in r.lrange("latest:"+ str(int(username)), 0, 2)]
    return render_template("rec.html", user=username, item0=items[0], item1=items[1], item2=items[2], message=msg)


if __name__ == '__main__':
    app.debug = True
    app.run()
