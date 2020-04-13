import psycopg2
import redis
import json
import os
from bottle import Bottle, request


class Sender(Bottle):
	def __init__(self):
		super().__init__()
		self.route('/', method='POST', callback=self.send)
		self.fila = redis.StrictRedis(host='queue', port=6379, db=0)

		db_host = os.getenv('DB_HOST', 'db')
		db_user = os.getenv('DB_USER', 'postgres')
		db_name = os.getenv('DB_NAME', 'sender')
		db_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
		dsn = f'dbname={db_name} user={db_user} host={db_host} password={db_password}'

		DSN = 'dbname=email_sender user=postgres host=db password=postgres'
		self.conn = psycopg2.connect(dsn)

	def register_message(self, assunto, mensagem):
		cur = self.conn.cursor()
		SQL = 'INSERT INTO emails (assunto, mensagem) VALUES (%s, %s)'
		cur.execute(SQL, (assunto, mensagem))
		self.conn.commit()
		cur.close()

		msg = {'assunto': assunto, 'mensagem': mensagem}
		self.fila.rpush('sender', json.dumps(msg))

		print("Mensagem registrada!")

	def send(self):
		assunto = request.forms.get('assunto')
		mensagem = request.forms.get('mensagem')

		self.register_message(assunto, mensagem)
		return f'Mensage enfileirada ! Assunto: {assunto} Mensagem: {mensagem}'

if __name__ == '__main__':
	sender = Sender()
	sender.run(host='0.0.0.0', port=8080, debug=True)