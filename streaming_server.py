import socketserver
import threading
import time
import logging


def start_server_thread():
    class MySocketHandler(socketserver.BaseRequestHandler):
        def handle(self):
            filetosend = open("data/census_2010.json", "r")
            for line in filetosend:
                self.request.sendall(line.encode("UTF-8"))
                time.sleep(0.1)

            filetosend.close()
            self.request.close()
    logging.info("Thread starting")
    socketserver.TCPServer(("127.0.0.1", 9999),
                           MySocketHandler).serve_forever()
    logging.info("Thread finishing")


if __name__ == "__main__":

    thread = threading.Thread(target=start_server_thread, daemon=True)
    thread.start()
    thread.join(100)
