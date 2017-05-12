import time
import sys
from concurrent import futures

import lm_score_pb2
import lm_query
import grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class LMScore(lm_score_pb2.LanguageModelServicer):
    def __init__(self, arpa_file):
        self.model = lm_query.LMScoring(arpa_file)

    def Score(self, request, context):
        print 'Received message: %s' % request
        score = self.model.query(request.sentence)
        return lm_score_pb2.LMScoreResponse(score=score)

def serve():
    if len(sys.argv) != 2:
        print("lm_server.py <arpa_file>")
        exit(0)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lm_score_pb2.add_LanguageModelServicer_to_server(LMScore(sys.argv[1]), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
