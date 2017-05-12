import grpc
import lm_score_pb2

_TIMEOUT_SECONDS = 10

def run(sent):
    print(sent)
    channel = grpc.insecure_channel('localhost:50051')
    print("connecting...")
    stub = lm_score_pb2.LanguageModelStub(channel)
    print("sending a message")
    response = stub.Score(lm_score_pb2.SentenceRequest(sentence=sent), _TIMEOUT_SECONDS)
    print 'Parser client received: %s' % response
    #print 'response.score=%s' % response.score

if __name__ == '__main__':
    while True:
        sent = raw_input()
        run(sent)
