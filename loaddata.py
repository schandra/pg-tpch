#/usr/bin/python

import time
from datetime import datetime
from multiprocessing import Process, Queue

import psycopg2

# pool size
nprocesses = 16
dbgen_path = "/home/postgres/pg-tpch/dbgen/"
dbgen_size = "30000"
db_port = '20002'
db_hosts = [
				"xlcloud2", "xlcloud3", "xlcloud4", "xlcloud5", "xlcloud6", "xlcloud7", "xlcloud8", "xlcloud9",
				"xlcloud10", "xlcloud11", "xlcloud12", "xlcloud13", "xlcloud14", "xlcloud15", "xlcloud16", "xlcloud17"
		]

db_connect_str = "dbname=tpch "

commands = [
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 1' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 2' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 3' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 4' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 5' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 6' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 7' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 8' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 1' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 2' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 3' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 4' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 5' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 6' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 7' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 8' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 9' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 10' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 11' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 12' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 13' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 14' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 15' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 16' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 9' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 10' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 11' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 12' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 13' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 14' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 15' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 16' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 17' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 18' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 19' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 20' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 21' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 22' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 23' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 24' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 17' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 18' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 19' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 20' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 21' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 22' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 23' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 24' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 25' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 26' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 27' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 28' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 29' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 30' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 31' DELIMITER '|';",
                                "COPY lineitem FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T L -o -b " + dbgen_path + "dists.dss -C 32 -S 32' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 25' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 26' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 27' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 28' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 29' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 30' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 31' DELIMITER '|';",
                                "COPY customer FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T c -o -b " + dbgen_path + "dists.dss -C 32 -S 32' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 1' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 2' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 3' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 4' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 5' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 6' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 7' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 8' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 9' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 10' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 11' DELIMITER '|';",
                                "COPY orders FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T O -o -b " + dbgen_path + "dists.dss -C 12 -S 12' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 1' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 2' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 3' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 4' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 5' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 6' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 7' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 8' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 9' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 10' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 11' DELIMITER '|';",
                                "COPY partsupp FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T S -o -b " + dbgen_path + "dists.dss -C 12 -S 12' DELIMITER '|';",
                                "COPY part FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T P -o -b " + dbgen_path + "dists.dss -C 3 -S 1' DELIMITER '|';",
                                "COPY part FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T P -o -b " + dbgen_path + "dists.dss -C 3 -S 2' DELIMITER '|';",
                                "COPY part FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T P -o -b " + dbgen_path + "dists.dss -C 3 -S 3' DELIMITER '|';",
                                "COPY supplier FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T s -o -b " + dbgen_path + "dists.dss -C 3 -S 1' DELIMITER '|';",
                                "COPY supplier FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T s -o -b " + dbgen_path + "dists.dss -C 3 -S 2' DELIMITER '|';",
                                "COPY supplier FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T s -o -b " + dbgen_path + "dists.dss -C 3 -S 3' DELIMITER '|';",
                                "COPY nation FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T n -o -b " + dbgen_path + "dists.dss' DELIMITER '|';",
                                "COPY region FROM PROGRAM '" + dbgen_path + "dbgen -s " + dbgen_size + " -T r -o -b " + dbgen_path + "dists.dss' DELIMITER '|';"
]



def run_commands(i, queue_in, queue_out):

		try:

				conn = psycopg2.connect(db_connect_str + 'host = ' + db_hosts[i] + ' port = ' + db_port)
				cur  = conn.cursor()

				# iterate over results from the queue
				while True:

						# by default we have nothing
						sql = None

						try:
								sql = queue_in.get_nowait()
						except:
							pass

						if sql is None:
							break

						# build the SQL query
						queue_out.put({'type' : 'START', 'time' : datetime.now().strftime('%y-%m-%d %H:%M:%S'), 'sql' : sql})

						start_time = time.time()

						cur.execute(sql)
						conn.commit()

						end_time = time.time()

						# put the output message
						queue_out.put({'type' : 'END', 'time' : datetime.now().strftime('%y-%m-%d %H:%M:%S'), 'sql' : sql, 'duration' : int(end_time - start_time)})

				# close the cursor/connection
				cur.close()
				conn.close()

		except Exception as ex:

				print "process %(id)s failed: %(msg)s" % {'id' : str(i), 'msg' : str(ex)}


def receive_messages(q):
		'receive messages'

		while True:

				m = q.get(True)

				# we only know EXIT and START/END messages

				if m['type'] == 'EXIT':
						break

				elif m['type'] == 'START':
						print "%(time)s %(type)s %(sql)s" % m

				elif m['type'] == 'END':
						print "%(time)s %(type)s %(sql)s %(duration)s" % m


if __name__ == '__main__':

		# input queue (with all the files to print)
		queue_in = Queue()

		# fill the queue (all the commands at once)
		[queue_in.put(c) for c in commands]

		# output messages (not to print garbage)
		queue_out = Queue()

		# create pool of the expected size
		workers = [Process(target=run_commands, args=(i,queue_in,queue_out)) for i in range(nprocesses)]

		# create the process printing messages
		printer = Process(target=receive_messages, args=(queue_out,))

		# start all the workers and printer thread
		[p.start() for p in workers]
		printer.start()

		# wait for all the workers to complete
		[p.join() for p in workers]

		# terminate the message receiver (send message, wait for termination)
		queue_out.put({'type' : 'EXIT'})
		printer.join()

