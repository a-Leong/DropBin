import socket
import sys
import pickle
import string
import os
import time
import select
import math

#OPCODES
_NONE = -1
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
_MAX_BUFFER_STORE = 5000000


"""returns message with size prepended"""
def prepend_size(msg):
    return str(len(msg)) + ';' + msg

"""returns message_size, fileName, OPCODE"""
def msg_interp(msg):

    fstDel = msg.find(";")
    sndDel = fstDel + msg[fstDel + 1:].find(";") + 1

    if fstDel == -1:
        return msg, None

    elif len(msg) == 12:
        return msg, _KILL

    msgSize = int(msg[:fstDel])
    fileName = msg[fstDel + 1:sndDel]
    data = msg[sndDel + 1:]

    #print [msgSize, fileName, data]

    try:
        data = int(data)

        return [msgSize, fileName, data], _INIT

    except ValueError:
        return [msgSize, fileName, data], _PAYLOAD

"""returns server socket"""
def createServerSocket(hostname, port):

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((hostname, port))
    s.listen(1)
    s.setblocking(0)
        
    return s

def main(argv):

    _RUNNING = True

    try:
        f = open("fk.pkl", "rb")
    except IOError:
        f = open("fk.pkl", "w+")

    try:
        database = pickle.load(f)
    except EOFError:
        database = {}

    f.close()

    hostname = argv[0]
    port = int(argv[1])

    #print "hostname: " + hostname
    #print "port: " + str(port)

    serversocket = createServerSocket(hostname, port)


    epoll = select.epoll()
    epoll.register(serversocket.fileno(), select.EPOLLIN)


    try:
        connections = {}    #dictionary of connection fileno : connection object
        chunks = {}         #dictionary of fileno : [chunk requirement, count]
        buffers = {}        #dictionary of fileno : [buffer size, update]
        statuses = {}       #dictionary of fileno : status
        names = {}          #dictionary of fileno : filename


        while _RUNNING:
            events = epoll.poll(1)
            for fileno, event in events:
                if fileno == serversocket.fileno():
                    connection, address = serversocket.accept()
                    connection.setblocking(0)

                    epoll.register(connection.fileno(), select.EPOLLIN)

                    #start connection timer
                    connections[connection.fileno()] = connection

                elif event & select.EPOLLIN:

                    try:
                        message = connections[fileno].recv(_CHUNK_SIZE)
                        msg, opcode = msg_interp(message)
                        print msg

                        if opcode == _INIT:
                            #print "INIT MESSAGE RECEIVED"
                            updateFileSize = int(msg[2])
                            fileName = msg[1]

                            names[fileno] = fileName

                            #create file and file entry if no such file entry exists
                            if not database.has_key(fileName):
                                database[fileName] = [0, os.path.abspath(fileName)]
                                open(fileName, "w+")

                                #print "adding file and database entry"

                            #database file is up to date
                            if database[fileName][0] >= updateFileSize:
                                statuses[fileno] = _ALREADY_HAVE
                                epoll.modify(fileno, select.EPOLLOUT)
                                
                                #print "File up to date"
                                

                            #database file needs to be updated
                            else:
                                #update file
                                statuses[fileno] = _READY_TO_RECEIVE
                                #print "file name: " + fileName
                                #print "file size: " + str(database[fileName][0])
                                dataChunkSize = float(_CHUNK_SIZE - _MAX_META_DATA)
                                nChunks = math.ceil(float(updateFileSize - database[fileName][0]) / dataChunkSize)
                                chunks[fileno] = [int(nChunks), 0]
                                buffers[fileno] = [0, ""]

                                #print "Ready to receive update"
                                epoll.modify(fileno, select.EPOLLOUT)

                        elif opcode == _PAYLOAD:
                            #print "PAYLOAD MESSAGE RECEIVED"

                            fileName = msg[1]
                            file = open(fileName, "a")

                            payload = msg[2] 

                            #clear buffer and write to disk if buffer over _MAX_BUFFER_STORE
                            if buffers[fileno][0] > _MAX_BUFFER_STORE:
                                file.write(buffers[fileno][1])

                                database[fileName][0] += buffers[fileno][0]

                                buffers[fileno] = [0, ""]
                                #print "file name: " + fileName
                                #print "new file size: " + str(database[fileName][0])

                            #last chunk received
                            if chunks[fileno][1] == chunks[fileno][0] - 1:
                                #SyncComplete
                                statuses[fileno] = _SYNC_COMPLETE
                                chunks[fileno][1] += 1

                                buffers[fileno][0] += len(payload)
                                buffers[fileno][1] += payload
                    
                                file.write(buffers[fileno][1])
                                database[fileName][0] += buffers[fileno][0]

                                #print "file name: " + fileName
                                #print "new file size: " + str(database[fileName][0])

                                epoll.modify(fileno, select.EPOLLOUT)

                            #write payload to buffer
                            elif chunks[fileno][0] > chunks[fileno][1]:

                                #print "listen for payload"
                                #print chunks[fileno][1]

                                buffers[fileno][1] += payload
                                chunks[fileno][1] += 1
                                buffers[fileno][0] += len(payload)

                                statuses[fileno] = _CHUNK_RECEIVED

                                #print "wrote payload to buffer"

                                epoll.modify(fileno, select.EPOLLOUT)

                            file.close()

                        elif opcode == _KILL:
                            #print "kill message receieved. shutting down."

                            epoll.modify(fileno, select.EPOLLHUP)
                            connections[fileno].shutdown(socket.SHUT_RDWR)
                            epoll.unregister(fileno)
                            connections[fileno].close()
                            del connections[fileno]

                    except IOError:
                        pass

                elif event & select.EPOLLOUT:

                    #print "EPOLLOUT"
                    #print statuses[fileno]

                    fileName = names[fileno]
                    if statuses[fileno] == _ALREADY_HAVE:
                        notify = fileName + ";OP_ALREADY_HAVE"
                        connections[fileno].sendall(prepend_size(notify))

                        epoll.modify(fileno, select.EPOLLHUP)

                    elif statuses[fileno] == _READY_TO_RECEIVE:
                        notify = fileName + ";OP_READY_TO_RECEIVE;" + str(database[fileName][0])
                        connections[fileno].sendall(prepend_size(notify))

                        epoll.modify(fileno, select.EPOLLIN)

                    elif statuses[fileno] == _SYNC_COMPLETE:
                        notify = fileName + ";OP_SYNC_COMPLETE;" + str(chunks[fileno][1])
                        connections[fileno].sendall(prepend_size(notify))

                        epoll.modify(fileno, select.EPOLLHUP)

                    elif statuses[fileno] == _CHUNK_RECEIVED:
                        notify = fileName + ";OP_CHUNK_RECEIVED;" + str(chunks[fileno][1])
                        connections[fileno].sendall(prepend_size(notify))

                        epoll.modify(fileno, select.EPOLLIN)
                       
                elif event & select.EPOLLHUP:
                    epoll.unregister(fileno)
                    connections[fileno].close()
                    del connections[fileno]
                    _RUNNING = False

    except KeyboardInterrupt:
    #    #print "\n==Server closed=="
        pass

    except:
    #    #print "Encountered unexpected error: ", sys.exc_info()[0]
       pass

    finally:
       epoll.unregister(serversocket.fileno())
       epoll.close()
       serversocket.close()

       f = open("fk.pkl", "w+")
       pickle.dump(database, f)


if __name__ == '__main__':

    if len(sys.argv) != 3:
        #print "Invalid number of command line arguments"
        pass
    else:
        main(sys.argv[1:])
