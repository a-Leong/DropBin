import socket
import sys
import time
import os
import string
import math

#OPCODES
_INIT = 0
_ALREADY_HAVE = 1
_READY_TO_RECEIVE = 2
_SYNC_COMPLETE = 3
_CHUNK_RECEIVED = 4
_PAYLOAD = 5
_KILL = 6

#GLOBALS
_CHUNK_SIZE = 8192
_MAX_META_DATA = 110
_DATA_CHUNK_SIZE = _CHUNK_SIZE - _MAX_META_DATA



def chunk_content(content):
	chunks = []
	while len(content) > 0:
		chunks.append(content[:_DATA_CHUNK_SIZE])
		if len(content) > _DATA_CHUNK_SIZE:
			content = content[_DATA_CHUNK_SIZE:]
		else:
			content = ""

	return chunks

def prepend_size(msg):
	return str(len(msg)) + ';' + msg

def msg_interp(msg):

	msg = string.split(msg, ';')

	if len(msg) < 3:
		#print "Receiving no message"
		return None, None

	if msg[2] == "OP_ALREADY_HAVE":
		return _ALREADY_HAVE, 0

	elif msg[2] == "OP_READY_TO_RECEIVE":
		return _READY_TO_RECEIVE, int(msg[3])

	elif msg[2] == "OP_SYNC_COMPLETE":
		return _SYNC_COMPLETE, int(msg[3])

	elif msg[2] == "OP_CHUNK_RECEIVED":
		return _CHUNK_RECEIVED, int(msg[3])

	else:
		#print "Unexpected message"
		return None, None

def update_backup(host, port, filename):

	try:
		f = open(filename, "rb")
	except:
		return

	fileContents = f.read()

	clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientSocket.connect((host, port))


	#print "\nconnection made\n"

	try:

		mInit = filename + ";" + str(os.path.getsize(filename))
		mInit = prepend_size(mInit)

		clientSocket.sendall(mInit)

		initResponse = clientSocket.recv(_CHUNK_SIZE)

		#print initResponse
		opcode, data = msg_interp(initResponse)

		if opcode == _ALREADY_HAVE:
			#print "File up to date"
			pass
		
		elif opcode == _READY_TO_RECEIVE:
			dataNeeded = fileContents[data:]
			chunkedData = chunk_content(dataNeeded)
			dataChunkSize = float(_CHUNK_SIZE - _MAX_META_DATA)
			nChunks = int(math.ceil(float(len(dataNeeded) / dataChunkSize)))

			#print "nChunks: ", nChunks
			chunk = 0
			data = 0

			while chunk < nChunks:

				if data == chunk:
					payload = filename + ';' + chunkedData[chunk]

					#print payload
					chunk += 1
					#print "send chunk"
					clientSocket.sendall(prepend_size(payload))
				#print "listen for response"

				payloadResponse = clientSocket.recv(_CHUNK_SIZE)
				opcode, data = msg_interp(payloadResponse)
				
	finally:
		#print "closing connection...\n"
		clientSocket.close()

def main(argv):
	update_backup(argv[0], int(argv[1]), argv[2])


if __name__ == '__main__':
	if len(sys.argv) != 4:
		#print "Invalid number of command line arguments"
		pass
	else:
		main(sys.argv[1:])

